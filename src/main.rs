use anyhow::{Context, Result};
use clap::Parser;
use mimalloc::MiMalloc;
use reqwest::Client;
use std::path::PathBuf;
use std::time::Duration;
use tokio::fs::File;
use tokio_util::io::ReaderStream;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;
#[derive(Parser, Debug)]
#[command(author, version, about = "ClickHouse 大文件高速导入工具")]
struct Args {
    #[arg(short, long, help = "文件路径")]
    file: PathBuf,

    #[arg(short, long, help = "目标表名")]
    table: String,

    #[arg(short, long, default_value = "http://127.0.0.1:8123")]
    url: String,

    #[arg(long, default_value = "default")]
    user: String,

    #[arg(long, default_value = "")]
    password: String,

    #[arg(long, default_value = "8", help = "CK服务端并行写入线程数")]
    threads: u32,

    #[arg(long, default_value = "8", help = "缓冲区大小MB")]
    cap: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // 1. 构造带有性能参数的 URL
    // input_format_parallel_parsing=1: 开启格式并行解析（对ORC至关重要）
    // max_insert_threads: 提升写入并发
    let query = format!("INSERT INTO {} FORMAT ORC", args.table);

    let target_url = format!(
        "{}/?query={}&input_format_parallel_parsing=1&max_insert_threads={}",
        args.url,
        urlencoding::encode(&query),
        args.threads
    );

    println!("🚀 开始加载文件: {:?}", args.file);
    println!("📅 目标表: {}", args.table);

    // 2. 准备文件流
    let file = File::open(&args.file)
        .await
        .with_context(|| format!("无法打开文件: {:?}", args.file))?;

    let stream = ReaderStream::with_capacity(file, (args.cap as usize) * 1024 * 1024); //  读缓冲区大小
    let body = reqwest::Body::wrap_stream(stream);

    // 3. 配置 HTTP 客户端
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(10))
            // 对于超大文件，给予更长的总超时时间
    .timeout(Duration::from_secs(7200)) 
        .tcp_keepalive(Duration::from_secs(60))
        .tcp_nodelay(true) // 减少延迟
        .build()?;

    // 4. 执行 POST 请求
    let start_time = std::time::Instant::now();
    let response = client
        .post(&target_url)
        .basic_auth(args.user, Some(args.password))
        .body(body)
        .send()
        .await
        .context("发送请求至 ClickHouse 失败")?;

    // 5. 结果检查
    if response.status().is_success() {
        let duration = start_time.elapsed();
        println!("✅ 加载成功！耗时: {:?}", duration);
    } else {
        let status = response.status();
        let error_body = response.text().await.unwrap_or_default();
        eprintln!("❌ 加载失败 (HTTP {}):", status);
        eprintln!("{}", error_body.chars().take(2000).collect::<String>());
        std::process::exit(1);
    }

    Ok(())
}
