## 一、启动 clickhouse docker 容器

```shell
docker run -d -p 18123:8123 -p19000:9000 -e CLICKHOUSE_PASSWORD=changeme --name some-clickhouse-server --ulimit nofile=262144:262144 clickhouse/clickhouse-server
echo 'SELECT version()' | curl 'http://localhost:18123/?password=changeme' --data-binary @-
```

 

## 二、rust程序设计 

针对大文件、高并发场景进行了精准优化

**1. 异步非阻塞 IO 架构 (Tokio + Reqwest)**

- **架构设计**：代码基于 `tokio` 异步运行时，利用 `reqwest` 的异步 HTTP 能力。
- **优势**：在上传几十 GB 的大文件时，传统的同步 IO 会导致线程阻塞，而异步 IO 允许单线程或少量线程同时处理更多的网络事件和磁盘读取。这确保了在数据传输过程中，程序能够以极低的资源占用维持高吞吐量。

**2. 零拷贝流式处理 (Stream-based Upload)**

- **优势**：
  - **内存恒定**：代码不会一次性将大文件加载到内存中（这是很多新手容易犯的错误）。它采用流式读取，每次只读取一小块（2MB 缓冲区）并立即发送到网络。
  - **低内存足迹**：无论导入的文件是 100MB 还是 100GB，该工具的内存占用（RSS）都会保持在一个非常低且稳定的水平。

**3. 自定义内存分配器 (MiMalloc)**

- **核心逻辑**：使用了 `mimalloc` 作为全局内存分配器。
- **优势**：
  - **减少竞争**：在处理像 ORC 这种需要大量细碎内存分配的格式时，标准库的 `malloc` 在多线程环境下容易出现锁竞争。
  - **碎片优化**：`mimalloc` 在处理大文件解析产生的临时对象时，内存回收效率更高，能显著提升长期运行时的性能，减少内存碎片导致的 OOM。

**4. ClickHouse 服务端性能压榨 (Query Parameters)**

代码通过 URL 参数直接控制 ClickHouse 服务端的行为，这是其“高速”的关键：

- **`input_format_parallel_parsing=1`**：这是导入 ORC 格式的“加速开关”。ORC 是列式存储且压缩率高，开启此参数后，ClickHouse 会利用多核 CPU 并行解压和解析数据，速度通常能提升数倍。
- **`max_insert_threads`**：手动指定服务端写入线程数，允许 ClickHouse 同时向多个 Data Part（数据分区）写入数据，最大化磁盘 IO 利用率。
- **`wait_for_async_insert`**：确保数据的可靠性，让客户端能够接收到真实的写入结果。

**5. 健壮性与生产级配置**

- **超时控制**：
  - `connect_timeout` 防止在网络不可达时无限等待。
  - `.timeout(Duration::from_secs(3600))` 取消了默认的 30 秒超时（这对大文件至关重要），确保长达一小时的导入任务不会被客户端自己掐掉。
- **HTTP Keep-alive**：配置 `tcp_keepalive` 减少了频繁建立 TCP 连接的握手开销。
- **错误上下文 (`Anyhow`)**：使用 `with_context` 链式记录错误信息，一旦出错，能清晰地知道是“文件打不开”、“URL 解析不对”还是“ClickHouse 内部报错”。

**6. 命令行交互设计 (Clap)**

- 使用 `clap` 的 `derive` 模式，自动生成帮助文档（`--help`），并提供默认值（如 URL、线程数、用户名）。这种声明式设计使得工具非常易于在 Shell 脚本或自动化流水线中集成。

总结

**该代码的架构精髓在于：**
它作为一个“轻量级网关”，在 **本地磁盘 IO**、**网络传输** 和 **远程数据库写入** 三者之间建立了一条高效的流水线（Pipeline）。它将繁重的计算任务（并行解析和写入）推给了 ClickHouse 集群，而本地则专注于极致的 IO 转发效率。

**性能表现预测：**
在千兆网络和高性能 SSD 场景下，该工具处理 ORC 文件的速度通常受限于 ClickHouse 节点的 CPU 解析能力，能够轻松跑满带宽。

## 三、跨平台编译 

### 安装zig

```pow
现在zip包

设置环境变量
System wide (admin Powershell):

[Environment]::SetEnvironmentVariable(
   "Path",
   [Environment]::GetEnvironmentVariable("Path", "Machine") + ";C:\Users\brace\Downloads\zig-x86_64-windows-0.16.0",
   "Machine"
)
```

### 编译

```power
# 编译出一个兼容 glibc 2.17 (CentOS 7 标准) 的二进制文件
cargo zigbuild --release --target x86_64-unknown-linux-gnu.2.17



如果你依然想坚持用 musl 静态链接,在安装了 zig 和 cargo-zigbuild 后，运行：
cargo zigbuild --release --target x86_64-unknown-linux-musl

x86_64-unknown-linux-gnu 和 x86_64-unknown-linux-musl
```

### **验证与传输**

1. 编译完成后，二进制文件位于：`target\x86_64-unknown-linux-gnu\release\你的程序名`。
2. 将其上传到 CentOS 7。
3. 在 CentOS 上运行 `ldd ./程序名`，如果没有显示 `GLIBC_2.XX not found`，则说明成功。