//! TCP connection handling for flashQ.
//!
//! Handles both text (JSON) and binary (MessagePack) protocols.

use std::sync::Arc;

use parking_lot::RwLock;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};

use crate::protocol::{create_binary_frame, serialize_msgpack, Response, ResponseWithId};
use crate::queue::QueueManager;

use super::handler::{process_command_binary, process_command_text};

/// Connection state tracking authentication
pub struct ConnectionState {
    pub authenticated: bool,
}

/// Handle a new TCP connection (auto-detects protocol)
pub async fn handle_connection<R, W>(
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
    let state = Arc::new(RwLock::new(ConnectionState {
        authenticated: false,
    }));

    // Peek first byte to detect protocol
    let first_byte = {
        let buf = reader.fill_buf().await?;
        if buf.is_empty() {
            return Ok(());
        }
        buf[0]
    };

    // Route to appropriate handler based on protocol
    if first_byte == b'{' || first_byte == b'\n' || first_byte == b'\r' {
        // Text protocol (JSON, line-delimited)
        handle_text_protocol(&mut reader, &mut writer, &queue_manager, &state).await
    } else {
        // Binary protocol (MessagePack, length-prefixed)
        handle_binary_protocol(&mut reader, &mut writer, &queue_manager, &state).await
    }
}

/// Handle text protocol (JSON, newline-delimited)
pub async fn handle_text_protocol<R, W>(
    reader: &mut BufReader<R>,
    writer: &mut BufWriter<W>,
    queue_manager: &Arc<QueueManager>,
    state: &Arc<RwLock<ConnectionState>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut line = String::with_capacity(8192);

    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line).await?;

        if bytes_read == 0 {
            break;
        }

        let response = process_command_text(&mut line, queue_manager, state).await;
        let response_json = serde_json::to_string(&response)?;
        writer.write_all(response_json.as_bytes()).await?;
        writer.write_all(b"\n").await?;

        // Pipelining: only flush if no more commands waiting in buffer
        if reader.buffer().is_empty() {
            writer.flush().await?;
        }
    }

    Ok(())
}

/// Handle binary protocol (MessagePack, length-prefixed frames)
/// Frame format: [4 bytes length (big-endian u32)] [N bytes MessagePack data]
pub async fn handle_binary_protocol<R, W>(
    reader: &mut BufReader<R>,
    writer: &mut BufWriter<W>,
    queue_manager: &Arc<QueueManager>,
    state: &Arc<RwLock<ConnectionState>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut len_buf = [0u8; 4];
    let mut data_buf = Vec::with_capacity(8192);

    loop {
        // Read 4-byte length prefix
        match reader.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }

        let len = u32::from_be_bytes(len_buf) as usize;

        // Sanity check: max 16MB per message
        if len > 16 * 1024 * 1024 {
            let err_response = ResponseWithId::new(Response::error("Message too large"), None);
            let err_bytes = serialize_msgpack(&err_response)?;
            let frame = create_binary_frame(&err_bytes);
            writer.write_all(&frame).await?;
            writer.flush().await?;
            continue;
        }

        // Read message data
        data_buf.clear();
        data_buf.resize(len, 0);
        reader.read_exact(&mut data_buf).await?;

        // Process command
        let response = process_command_binary(&data_buf, queue_manager, state).await;

        // Serialize and send response
        let response_bytes = serialize_msgpack(&response)?;
        let frame = create_binary_frame(&response_bytes);
        writer.write_all(&frame).await?;

        // Pipelining: only flush if no more commands waiting in buffer
        if reader.buffer().is_empty() {
            writer.flush().await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tokio::io::duplex;

    /// Test ConnectionState default values
    #[test]
    fn test_connection_state_default() {
        let state = ConnectionState {
            authenticated: false,
        };
        assert!(!state.authenticated);
    }

    /// Test ConnectionState authenticated
    #[test]
    fn test_connection_state_authenticated() {
        let state = ConnectionState {
            authenticated: true,
        };
        assert!(state.authenticated);
    }

    /// Test protocol detection: JSON starts with '{'
    #[tokio::test]
    async fn test_protocol_detection_json() {
        let qm = QueueManager::new(false);
        let input = b"{\"cmd\":\"STATS\"}\n";
        let reader = Cursor::new(input.to_vec());
        let mut output = Vec::new();

        let result = handle_connection(reader, &mut output, qm).await;
        assert!(result.is_ok());

        // Output should be JSON (contains "ok")
        let output_str = String::from_utf8_lossy(&output);
        assert!(output_str.contains("\"ok\""));
    }

    /// Test protocol detection: newline triggers text mode
    #[tokio::test]
    async fn test_protocol_detection_newline() {
        let qm = QueueManager::new(false);
        let input = b"\n{\"cmd\":\"STATS\"}\n";
        let reader = Cursor::new(input.to_vec());
        let mut output = Vec::new();

        let result = handle_connection(reader, &mut output, qm).await;
        assert!(result.is_ok());
    }

    /// Test protocol detection: carriage return triggers text mode
    #[tokio::test]
    async fn test_protocol_detection_cr() {
        let qm = QueueManager::new(false);
        let input = b"\r\n{\"cmd\":\"STATS\"}\n";
        let reader = Cursor::new(input.to_vec());
        let mut output = Vec::new();

        let result = handle_connection(reader, &mut output, qm).await;
        assert!(result.is_ok());
    }

    /// Test empty connection (no data)
    #[tokio::test]
    async fn test_empty_connection() {
        let qm = QueueManager::new(false);
        let input: &[u8] = b"";
        let reader = Cursor::new(input.to_vec());
        let mut output = Vec::new();

        let result = handle_connection(reader, &mut output, qm).await;
        assert!(result.is_ok());
        assert!(output.is_empty());
    }

    /// Test text protocol with multiple commands (pipelining)
    #[tokio::test]
    async fn test_text_protocol_pipelining() {
        let qm = QueueManager::new(false);
        let input = b"{\"cmd\":\"STATS\"}\n{\"cmd\":\"STATS\"}\n";
        let reader = Cursor::new(input.to_vec());
        let mut output = Vec::new();

        let result = handle_connection(reader, &mut output, qm).await;
        assert!(result.is_ok());

        // Should have two JSON responses
        let output_str = String::from_utf8_lossy(&output);
        let responses: Vec<&str> = output_str.trim().split('\n').collect();
        assert_eq!(responses.len(), 2);
    }

    /// Test text protocol with invalid JSON
    #[tokio::test]
    async fn test_text_protocol_invalid_json() {
        let qm = QueueManager::new(false);
        let input = b"{invalid json}\n";
        let reader = Cursor::new(input.to_vec());
        let mut output = Vec::new();

        let result = handle_connection(reader, &mut output, qm).await;
        assert!(result.is_ok());

        // Should return error response
        let output_str = String::from_utf8_lossy(&output);
        assert!(output_str.contains("\"ok\":false"));
    }

    /// Test binary protocol detection (non-JSON first byte)
    #[tokio::test]
    async fn test_protocol_detection_binary() {
        let qm = QueueManager::new(false);

        // Create a valid MessagePack request
        use crate::protocol::{create_binary_frame, serialize_msgpack, Command, Request};

        let request = Request {
            req_id: None,
            command: Command::Stats,
        };
        let msgpack_data = serialize_msgpack(&request).unwrap();
        let frame = create_binary_frame(&msgpack_data);

        let reader = Cursor::new(frame);
        let mut output = Vec::new();

        let result = handle_connection(reader, &mut output, qm).await;
        assert!(result.is_ok());

        // Output should be a binary frame (starts with length prefix)
        assert!(output.len() >= 4);
    }

    /// Test binary protocol with multiple frames
    #[tokio::test]
    async fn test_binary_protocol_multiple_frames() {
        let qm = QueueManager::new(false);

        use crate::protocol::{create_binary_frame, serialize_msgpack, Command, Request};

        let request = Request {
            req_id: None,
            command: Command::Stats,
        };
        let msgpack_data = serialize_msgpack(&request).unwrap();

        // Send two frames
        let mut input = Vec::new();
        input.extend_from_slice(&create_binary_frame(&msgpack_data));
        input.extend_from_slice(&create_binary_frame(&msgpack_data));

        let reader = Cursor::new(input);
        let mut output = Vec::new();

        let result = handle_connection(reader, &mut output, qm).await;
        assert!(result.is_ok());

        // Should have two response frames
        // Each frame has 4-byte length prefix + data
        assert!(output.len() > 8); // At least two frames
    }

    /// Test binary protocol message too large error
    #[tokio::test]
    async fn test_binary_protocol_message_too_large() {
        let qm = QueueManager::new(false);

        // Create a frame with length > 16MB
        let mut input = Vec::new();
        let huge_len: u32 = 17 * 1024 * 1024; // 17MB
        input.extend_from_slice(&huge_len.to_be_bytes());
        // Don't actually add the data, just the length prefix
        // Add some dummy data to prevent immediate EOF
        input.extend_from_slice(&[0u8; 100]);

        let reader = Cursor::new(input);
        let mut output = Vec::new();

        // This should return an error response, not crash
        let _result = handle_connection(reader, &mut output, qm).await;
        // The result might be an error due to EOF when trying to read the huge message
        // but it should not panic

        // If we got a response, it should be an error
        if !output.is_empty() {
            assert!(output.len() >= 4);
        }
    }

    /// Test text protocol PUSH and PULL
    #[tokio::test]
    async fn test_text_protocol_push_pull() {
        let qm = QueueManager::new(false);
        let input =
            b"{\"cmd\":\"PUSH\",\"queue\":\"test\",\"data\":{\"x\":1}}\n{\"cmd\":\"STATS\"}\n";
        let reader = Cursor::new(input.to_vec());
        let mut output = Vec::new();

        let result = handle_connection(reader, &mut output, qm).await;
        assert!(result.is_ok());

        let output_str = String::from_utf8_lossy(&output);
        // First response should be ok with id
        assert!(output_str.contains("\"ok\":true"));
        assert!(output_str.contains("\"id\":"));
    }

    /// Test duplex stream simulation
    #[tokio::test]
    async fn test_duplex_connection() {
        let qm = QueueManager::new(false);
        let (client, server) = duplex(1024);
        let (server_reader, server_writer) = tokio::io::split(server);
        let (mut client_reader, mut client_writer) = tokio::io::split(client);

        // Spawn server handler
        let qm_clone = qm.clone();
        let server_handle =
            tokio::spawn(
                async move { handle_connection(server_reader, server_writer, qm_clone).await },
            );

        // Send a command from client
        client_writer
            .write_all(b"{\"cmd\":\"STATS\"}\n")
            .await
            .unwrap();
        client_writer.flush().await.unwrap();

        // Read response
        let mut response = vec![0u8; 1024];
        let n = client_reader.read(&mut response).await.unwrap();
        assert!(n > 0);

        let response_str = String::from_utf8_lossy(&response[..n]);
        assert!(response_str.contains("\"ok\":true"));

        // Close client to end server
        drop(client_writer);
        drop(client_reader);

        // Wait for server to finish
        let _ = server_handle.await;
    }
}
