mod protocol;
mod queue;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, UnixListener};

use protocol::{Command, Response};
use queue::QueueManager;

const DEFAULT_PORT: u16 = 6789;
const UNIX_SOCKET_PATH: &str = "/tmp/magic-queue.sock";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let use_unix = std::env::var("UNIX_SOCKET").is_ok();
    let persistence = std::env::var("PERSIST").is_ok();

    let queue_manager = QueueManager::new(persistence);

    if persistence {
        println!("Persistence enabled (WAL)");
    }

    if use_unix {
        let _ = std::fs::remove_file(UNIX_SOCKET_PATH);
        let listener = UnixListener::bind(UNIX_SOCKET_PATH)?;
        println!("MagicQueue server listening on {}", UNIX_SOCKET_PATH);

        loop {
            let (socket, _) = listener.accept().await?;
            let qm = Arc::clone(&queue_manager);
            tokio::spawn(async move {
                let (reader, writer) = socket.into_split();
                let _ = handle_connection(reader, writer, qm).await;
            });
        }
    } else {
        let port = std::env::var("PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(DEFAULT_PORT);

        let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
        println!("MagicQueue server listening on port {}", port);

        loop {
            let (socket, _) = listener.accept().await?;
            socket.set_nodelay(true)?;
            let qm = Arc::clone(&queue_manager);
            tokio::spawn(async move {
                let (reader, writer) = socket.into_split();
                let _ = handle_connection(reader, writer, qm).await;
            });
        }
    }
}

async fn handle_connection<R, W>(
    reader: R,
    writer: W,
    queue_manager: Arc<QueueManager>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut reader = BufReader::with_capacity(128 * 1024, reader);
    let mut writer = BufWriter::with_capacity(128 * 1024, writer);
    let mut line = String::with_capacity(8192);

    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line).await?;

        if bytes_read == 0 {
            break;
        }

        let response = process_command(&line, &queue_manager).await;
        let response_json = serde_json::to_string(&response)?;
        writer.write_all(response_json.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
    }

    Ok(())
}

#[inline(always)]
async fn process_command(line: &str, queue_manager: &Arc<QueueManager>) -> Response {
    let command: Command = match serde_json::from_str(line.trim()) {
        Ok(cmd) => cmd,
        Err(e) => return Response::error(format!("Invalid: {}", e)),
    };

    match command {
        Command::Push {
            queue,
            data,
            priority,
            delay,
        } => {
            let job = queue_manager.push(queue, data, priority, delay).await;
            Response::ok_with_id(job.id)
        }
        Command::Pushb { queue, jobs } => {
            let ids = queue_manager.push_batch(queue, jobs).await;
            Response::batch(ids)
        }
        Command::Pull { queue } => {
            let job = queue_manager.pull(&queue).await;
            Response::job(job)
        }
        Command::Pullb { queue, count } => {
            let jobs = queue_manager.pull_batch(&queue, count).await;
            Response::jobs(jobs)
        }
        Command::Ack { id } => match queue_manager.ack(id).await {
            Ok(()) => Response::ok(),
            Err(e) => Response::error(e),
        },
        Command::Ackb { ids } => {
            let count = queue_manager.ack_batch(&ids).await;
            Response::batch(vec![count as u64])
        }
        Command::Fail { id, error } => match queue_manager.fail(id, error).await {
            Ok(()) => Response::ok(),
            Err(e) => Response::error(e),
        },
        Command::Stats => {
            let (queued, processing, delayed) = queue_manager.stats().await;
            Response::stats(queued, processing, delayed)
        }
    }
}
