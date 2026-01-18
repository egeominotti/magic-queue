use std::time::Instant;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

const SERVER_ADDR: &str = "127.0.0.1:6789";
const WARMUP_JOBS: usize = 1000;
const BENCHMARK_JOBS: usize = 50000;
const BATCH_SIZE: usize = 1000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║                 flashQ Benchmark Suite                       ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    // Connect to server
    let stream = TcpStream::connect(SERVER_ADDR).await?;
    stream.set_nodelay(true)?;
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    println!("Connected to {}\n", SERVER_ADDR);

    // Warmup
    println!("Warming up with {} jobs...", WARMUP_JOBS);
    for i in 0..WARMUP_JOBS {
        let cmd = format!(r#"{{"cmd":"PUSH","queue":"warmup","data":{{"i":{}}}}}"#, i);
        writer.write_all(cmd.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
        line.clear();
        reader.read_line(&mut line).await?;
    }
    println!("Warmup complete.\n");

    // ═══════════════════════════════════════════════════════════════
    // Benchmark 1: Sequential Push
    // ═══════════════════════════════════════════════════════════════
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("Benchmark 1: Sequential Push ({} jobs)", BENCHMARK_JOBS);
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let start = Instant::now();
    for i in 0..BENCHMARK_JOBS {
        let cmd = format!(r#"{{"cmd":"PUSH","queue":"bench1","data":{{"i":{}}}}}"#, i);
        writer.write_all(cmd.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
        line.clear();
        reader.read_line(&mut line).await?;
    }
    let elapsed = start.elapsed();
    let ops_per_sec = BENCHMARK_JOBS as f64 / elapsed.as_secs_f64();
    let latency_us = elapsed.as_micros() as f64 / BENCHMARK_JOBS as f64;

    println!("  Duration:     {:?}", elapsed);
    println!("  Throughput:   {:.0} ops/sec", ops_per_sec);
    println!("  Avg Latency:  {:.1} µs\n", latency_us);

    // ═══════════════════════════════════════════════════════════════
    // Benchmark 2: Batch Push
    // ═══════════════════════════════════════════════════════════════
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!(
        "Benchmark 2: Batch Push ({} jobs, batch size {})",
        BENCHMARK_JOBS, BATCH_SIZE
    );
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let batches = BENCHMARK_JOBS / BATCH_SIZE;
    let start = Instant::now();
    for batch in 0..batches {
        let jobs: Vec<String> = (0..BATCH_SIZE)
            .map(|i| format!(r#"{{"data":{{"batch":{},"i":{}}}}}"#, batch, i))
            .collect();
        let cmd = format!(
            r#"{{"cmd":"PUSHB","queue":"bench2","jobs":[{}]}}"#,
            jobs.join(",")
        );
        writer.write_all(cmd.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
        line.clear();
        reader.read_line(&mut line).await?;
    }
    let elapsed = start.elapsed();
    let ops_per_sec = BENCHMARK_JOBS as f64 / elapsed.as_secs_f64();

    println!("  Duration:     {:?}", elapsed);
    println!("  Throughput:   {:.0} ops/sec", ops_per_sec);
    println!("  Batches:      {} x {} jobs\n", batches, BATCH_SIZE);

    // ═══════════════════════════════════════════════════════════════
    // Benchmark 3: GetState (O(1) lookup test)
    // ═══════════════════════════════════════════════════════════════
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!(
        "Benchmark 3: GetState O(1) Lookup ({} lookups)",
        BENCHMARK_JOBS
    );
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // First push some jobs to have IDs to look up
    let mut job_ids: Vec<u64> = Vec::with_capacity(10000);
    for i in 0..10000 {
        let cmd = format!(r#"{{"cmd":"PUSH","queue":"bench3","data":{{"i":{}}}}}"#, i);
        writer.write_all(cmd.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
        line.clear();
        reader.read_line(&mut line).await?;
        // Parse job ID from response
        if let Some(id) = line
            .split("\"id\":")
            .nth(1)
            .and_then(|s| s.split('}').next())
        {
            if let Ok(id) = id.trim().parse::<u64>() {
                job_ids.push(id);
            }
        }
    }

    let lookups = BENCHMARK_JOBS.min(job_ids.len() * 5);
    let start = Instant::now();
    for i in 0..lookups {
        let id = job_ids[i % job_ids.len()];
        let cmd = format!(r#"{{"cmd":"GETSTATE","id":{}}}"#, id);
        writer.write_all(cmd.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
        line.clear();
        reader.read_line(&mut line).await?;
    }
    let elapsed = start.elapsed();
    let ops_per_sec = lookups as f64 / elapsed.as_secs_f64();
    let latency_us = elapsed.as_micros() as f64 / lookups as f64;

    println!("  Duration:     {:?}", elapsed);
    println!("  Throughput:   {:.0} ops/sec", ops_per_sec);
    println!("  Avg Latency:  {:.1} µs\n", latency_us);

    // ═══════════════════════════════════════════════════════════════
    // Benchmark 4: Full Cycle (Push → Pull → Ack)
    // ═══════════════════════════════════════════════════════════════
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("Benchmark 4: Full Cycle Push→Pull→Ack ({} jobs)", 10000);
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let cycles = 10000;
    let start = Instant::now();
    for i in 0..cycles {
        // Push
        let cmd = format!(r#"{{"cmd":"PUSH","queue":"bench4","data":{{"i":{}}}}}"#, i);
        writer.write_all(cmd.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
        line.clear();
        reader.read_line(&mut line).await?;
    }

    // Pull and ack
    for _ in 0..cycles {
        // Pull
        let cmd = r#"{"cmd":"PULL","queue":"bench4"}"#;
        writer.write_all(cmd.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
        line.clear();
        reader.read_line(&mut line).await?;

        // Parse job ID
        if let Some(id) = line
            .split("\"id\":")
            .nth(1)
            .and_then(|s| s.split(',').next())
        {
            if let Ok(id) = id.trim().parse::<u64>() {
                // Ack
                let cmd = format!(r#"{{"cmd":"ACK","id":{}}}"#, id);
                writer.write_all(cmd.as_bytes()).await?;
                writer.write_all(b"\n").await?;
                writer.flush().await?;
                line.clear();
                reader.read_line(&mut line).await?;
            }
        }
    }
    let elapsed = start.elapsed();
    let ops_per_sec = (cycles * 3) as f64 / elapsed.as_secs_f64(); // 3 ops per cycle
    let cycles_per_sec = cycles as f64 / elapsed.as_secs_f64();

    println!("  Duration:     {:?}", elapsed);
    println!("  Throughput:   {:.0} ops/sec (push+pull+ack)", ops_per_sec);
    println!("  Cycles/sec:   {:.0}\n", cycles_per_sec);

    // ═══════════════════════════════════════════════════════════════
    // Benchmark 5: True Pipelining (send all, then read all)
    // ═══════════════════════════════════════════════════════════════
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("Benchmark 5: True Pipelining ({} commands)", BENCHMARK_JOBS);
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let start = Instant::now();

    // Send all commands WITHOUT waiting for response
    for i in 0..BENCHMARK_JOBS {
        let cmd = format!(r#"{{"cmd":"PUSH","queue":"bench5","data":{{"i":{}}}}}"#, i);
        writer.write_all(cmd.as_bytes()).await?;
        writer.write_all(b"\n").await?;
    }
    writer.flush().await?;

    // Now read all responses
    for _ in 0..BENCHMARK_JOBS {
        line.clear();
        reader.read_line(&mut line).await?;
    }

    let elapsed = start.elapsed();
    let ops_per_sec = BENCHMARK_JOBS as f64 / elapsed.as_secs_f64();
    let latency_us = elapsed.as_micros() as f64 / BENCHMARK_JOBS as f64;

    println!("  Duration:     {:?}", elapsed);
    println!("  Throughput:   {:.0} ops/sec", ops_per_sec);
    println!("  Avg Latency:  {:.1} µs\n", latency_us);

    // ═══════════════════════════════════════════════════════════════
    // Get Stats
    // ═══════════════════════════════════════════════════════════════
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("Server Stats");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let cmd = r#"{"cmd":"METRICS"}"#;
    writer.write_all(cmd.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    line.clear();
    reader.read_line(&mut line).await?;

    // Pretty print metrics
    if line.contains("total_pushed") {
        let total_pushed: u64 = line
            .split("\"total_pushed\":")
            .nth(1)
            .and_then(|s| s.split(',').next())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let total_completed: u64 = line
            .split("\"total_completed\":")
            .nth(1)
            .and_then(|s| s.split(',').next())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let total_failed: u64 = line
            .split("\"total_failed\":")
            .nth(1)
            .and_then(|s| s.split(',').next())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let avg_latency: f64 = line
            .split("\"avg_latency_ms\":")
            .nth(1)
            .and_then(|s| s.split(',').next())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);

        println!("  Total Pushed:    {}", total_pushed);
        println!("  Total Completed: {}", total_completed);
        println!("  Total Failed:    {}", total_failed);
        println!("  Avg Latency:     {:.2} ms\n", avg_latency);
    }

    // ═══════════════════════════════════════════════════════════════
    // Benchmark 6: KV Set (Sequential)
    // ═══════════════════════════════════════════════════════════════
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("Benchmark 6: KV Set ({} keys)", BENCHMARK_JOBS);
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let start = Instant::now();
    for i in 0..BENCHMARK_JOBS {
        let cmd = format!(
            r#"{{"cmd":"KVSET","key":"bench:key:{}","value":{{"data":"test{}"}}}}"#,
            i, i
        );
        writer.write_all(cmd.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
        line.clear();
        reader.read_line(&mut line).await?;
    }
    let elapsed = start.elapsed();
    let ops_per_sec = BENCHMARK_JOBS as f64 / elapsed.as_secs_f64();
    let latency_us = elapsed.as_micros() as f64 / BENCHMARK_JOBS as f64;

    println!("  Duration:     {:?}", elapsed);
    println!("  Throughput:   {:.0} ops/sec", ops_per_sec);
    println!("  Avg Latency:  {:.1} µs\n", latency_us);

    // ═══════════════════════════════════════════════════════════════
    // Benchmark 7: KV Get (Sequential)
    // ═══════════════════════════════════════════════════════════════
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("Benchmark 7: KV Get ({} keys)", BENCHMARK_JOBS);
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let start = Instant::now();
    for i in 0..BENCHMARK_JOBS {
        let cmd = format!(r#"{{"cmd":"KVGET","key":"bench:key:{}"}}"#, i);
        writer.write_all(cmd.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
        line.clear();
        reader.read_line(&mut line).await?;
    }
    let elapsed = start.elapsed();
    let ops_per_sec = BENCHMARK_JOBS as f64 / elapsed.as_secs_f64();
    let latency_us = elapsed.as_micros() as f64 / BENCHMARK_JOBS as f64;

    println!("  Duration:     {:?}", elapsed);
    println!("  Throughput:   {:.0} ops/sec", ops_per_sec);
    println!("  Avg Latency:  {:.1} µs\n", latency_us);

    // ═══════════════════════════════════════════════════════════════
    // Benchmark 8: KV Pipelining (send all, then read all)
    // ═══════════════════════════════════════════════════════════════
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!(
        "Benchmark 8: KV Pipelining SET ({} commands)",
        BENCHMARK_JOBS
    );
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let start = Instant::now();

    // Send all commands WITHOUT waiting for response
    for i in 0..BENCHMARK_JOBS {
        let cmd = format!(
            r#"{{"cmd":"KVSET","key":"pipe:key:{}","value":{{"n":{}}}}}"#,
            i, i
        );
        writer.write_all(cmd.as_bytes()).await?;
        writer.write_all(b"\n").await?;
    }
    writer.flush().await?;

    // Now read all responses
    for _ in 0..BENCHMARK_JOBS {
        line.clear();
        reader.read_line(&mut line).await?;
    }

    let elapsed = start.elapsed();
    let ops_per_sec = BENCHMARK_JOBS as f64 / elapsed.as_secs_f64();
    let latency_us = elapsed.as_micros() as f64 / BENCHMARK_JOBS as f64;

    println!("  Duration:     {:?}", elapsed);
    println!("  Throughput:   {:.0} ops/sec", ops_per_sec);
    println!("  Avg Latency:  {:.1} µs\n", latency_us);

    // ═══════════════════════════════════════════════════════════════
    // Benchmark 9: KV INCR (Atomic Counter)
    // ═══════════════════════════════════════════════════════════════
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!(
        "Benchmark 9: KV INCR Atomic Counter ({} ops)",
        BENCHMARK_JOBS
    );
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let start = Instant::now();
    for _ in 0..BENCHMARK_JOBS {
        let cmd = r#"{"cmd":"KVINCR","key":"counter:bench"}"#;
        writer.write_all(cmd.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
        line.clear();
        reader.read_line(&mut line).await?;
    }
    let elapsed = start.elapsed();
    let ops_per_sec = BENCHMARK_JOBS as f64 / elapsed.as_secs_f64();
    let latency_us = elapsed.as_micros() as f64 / BENCHMARK_JOBS as f64;

    println!("  Duration:     {:?}", elapsed);
    println!("  Throughput:   {:.0} ops/sec", ops_per_sec);
    println!("  Avg Latency:  {:.1} µs\n", latency_us);

    // Verify counter value
    let cmd = r#"{"cmd":"KVGET","key":"counter:bench"}"#;
    writer.write_all(cmd.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    line.clear();
    reader.read_line(&mut line).await?;
    println!("  Final counter value: {}", line.trim());

    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║                    Benchmark Complete!                       ║");
    println!("╚══════════════════════════════════════════════════════════════╝");

    Ok(())
}
