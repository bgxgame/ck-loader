use anyhow::{Context, Result};
use clap::Parser;
use futures::future::join_all;
use mimalloc::MiMalloc;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Instant;
use tokio::process::Command;
use tokio::sync::Semaphore;
use tokio::time::{self, Duration};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Parser, Debug)]
#[command(
    author = "hjd",
    version = "v0.3",
    about = "ClickHouse 原生多线程并行加载工具 (生产优化版)"
)]
struct Args {
    #[arg(short, long, help = "包含 ORC 文件的目录")]
    dir: PathBuf,

    #[arg(short, long, help = "目标表名")]
    table: String,

    #[arg(long, default_value = "123")]
    password: String,

    #[arg(short, long, default_value = "4", help = "最大并行文件数")]
    workers: usize,

    #[arg(long, default_value = "8", help = "单个文件的解析线程数")]
    threads: usize,

    #[arg(long, default_value = "1800", help = "单个文件导入超时时间(秒)")]
    timeout_secs: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let start_time = Instant::now();

    // 1. 获取所有 ORC 文件列表
    let mut files = Vec::new();
    let entries =
        std::fs::read_dir(&args.dir).with_context(|| format!("无法读取目录: {:?}", args.dir))?;

    for entry in entries {
        let path = entry?.path();
        if path.is_file() {
            files.push(path);
        }
    }

    let total_files = files.len();
    if total_files == 0 {
        println!("📭 未找到 .orc 文件，程序退出。");
        return Ok(());
    }

    println!(
        "📂 找到 {} 个文件，准备执行 (并行数: {}, 解析线程: {})...",
        total_files, args.workers, args.threads
    );

    // 2. 环境准备：创建 done 目录
    let mut done_dir = args.dir.clone();
    done_dir.push("done");
    if !done_dir.exists() {
        std::fs::create_dir_all(&done_dir).context("无法创建 done 目录")?;
    }

    // 3. 构造共享资源
    let semaphore = Arc::new(Semaphore::new(args.workers));
    let args_arc = Arc::new(args);
    let mut tasks = Vec::new();

    for file_path in files {
        let sem = Arc::clone(&semaphore);
        let cfg = Arc::clone(&args_arc);
        let d_dir = done_dir.clone();

        let task = tokio::spawn(async move {
            let file_name = file_path.file_name().unwrap().to_string_lossy().to_string();

            // --- 核心点：只有拿到许可后才开始操作 IO ---
            let _permit = sem.acquire().await.expect("信号量异常");

            let start_task = Instant::now();
            println!("🚀 正在启动: {}", file_name);

            if !file_path.exists() {
                return;
            }

            // 打开文件句柄
            let file_handle = match std::fs::File::open(&file_path) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("❌ 无法打开文件 {}: {}", file_name, e);
                    return;
                }
            };

            // 4. 准备异步命令
            let mut child = Command::new("nice")
                .arg("-n")
                .arg("10")
                .arg("clickhouse-client")
                .arg("--password")
                .arg(&cfg.password)
                .arg("--input_format_parallel_parsing")
                .arg("1")
                .arg("--max_insert_threads")
                .arg(cfg.threads.to_string())
                .arg("-q")
                .arg(format!("INSERT INTO {} FORMAT ORC", cfg.table))
                .stdin(Stdio::from(file_handle))
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()
                .expect("无法启动 clickhouse-client 进程");

            let timeout_dur = Duration::from_secs(cfg.timeout_secs);

            // 5. 使用 select! 进行超时与状态监听
            let result = tokio::select! {
                res = child.wait() => {
                    match res {
                        Ok(status) if status.success() => Ok(()),
                        Ok(status) => {
                            // 失败时提取 stderr
                            let output = child.wait_with_output().await.ok();
                            let err_msg = output.map(|o| String::from_utf8_lossy(&o.stderr).to_string())
                                                .unwrap_or_else(|| format!("退出代码: {:?}", status.code()));
                            Err(err_msg)
                        },
                        Err(e) => Err(e.to_string()),
                    }
                }
                _ = time::sleep(timeout_dur) => {
                    let _ = child.kill().await;
                    Err(format!("⏰ 导入超时 (已运行超过 {:?})", timeout_dur))
                }
            };

            // 6. 结果处理
            match result {
                Ok(_) => {
                    println!(
                        "✅ SUCCESS: {} | 耗时: {:.2?}",
                        file_name,
                        start_task.elapsed()
                    );

                    // 移动到 done 目录
                    let mut target_path = d_dir;
                    target_path.push(&file_name);
                    if let Err(e) = std::fs::rename(&file_path, &target_path) {
                        eprintln!("⚠️ 成功后文件移动失败: {}, 错误: {}", file_name, e);
                    }
                }
                Err(e) => {
                    eprintln!("❌ ERROR: {} | 详情: {}", file_name, e.trim());
                }
            }
        });
        tasks.push(task);
    }

    // 7. 等待所有 Worker 完成
    join_all(tasks).await;

    println!("\n🏁 批次执行完毕！");
    println!("⏱️ 总耗时: {:.2?}", start_time.elapsed());

    Ok(())
}
