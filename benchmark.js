import net from 'net';

const HOST = 'localhost';
const PORT = 6789;

function createConnection() {
    return new Promise((resolve, reject) => {
        const client = new net.Socket();
        client.connect(PORT, HOST, () => resolve(client));
        client.on('error', reject);
    });
}

function sendCommand(client, cmd) {
    return new Promise((resolve) => {
        client.once('data', (data) => resolve(JSON.parse(data.toString())));
        client.write(JSON.stringify(cmd) + '\n');
    });
}

async function benchmarkPush(count) {
    const client = await createConnection();
    const start = Date.now();

    for (let i = 0; i < count; i++) {
        await sendCommand(client, {
            cmd: 'PUSH',
            queue: 'bench',
            data: { id: i, payload: 'x'.repeat(100) }
        });
    }

    const elapsed = Date.now() - start;
    client.destroy();
    return { count, elapsed, opsPerSec: Math.round(count / (elapsed / 1000)) };
}

async function benchmarkPushPull(count) {
    const client = await createConnection();
    const start = Date.now();

    for (let i = 0; i < count; i++) {
        await sendCommand(client, {
            cmd: 'PUSH',
            queue: 'bench2',
            data: { id: i }
        });
        const res = await sendCommand(client, { cmd: 'PULL', queue: 'bench2' });
        if (res.job) {
            await sendCommand(client, { cmd: 'ACK', id: res.job.id });
        }
    }

    const elapsed = Date.now() - start;
    client.destroy();
    return { count, elapsed, opsPerSec: Math.round(count / (elapsed / 1000)) };
}

async function benchmarkBatchPush(batchSize, batches) {
    const client = await createConnection();
    const start = Date.now();

    for (let b = 0; b < batches; b++) {
        const jobs = [];
        for (let i = 0; i < batchSize; i++) {
            jobs.push({ data: { id: b * batchSize + i }, priority: 0 });
        }
        await sendCommand(client, { cmd: 'PUSHB', queue: 'bench3', jobs });
    }

    const totalJobs = batchSize * batches;
    const elapsed = Date.now() - start;
    client.destroy();
    return { count: totalJobs, elapsed, opsPerSec: Math.round(totalJobs / (elapsed / 1000)) };
}

async function benchmarkConcurrent(connections, jobsPerConn) {
    const clients = await Promise.all(
        Array(connections).fill().map(() => createConnection())
    );

    const start = Date.now();

    await Promise.all(clients.map(async (client, idx) => {
        for (let i = 0; i < jobsPerConn; i++) {
            await sendCommand(client, {
                cmd: 'PUSH',
                queue: 'bench4',
                data: { conn: idx, job: i }
            });
        }
    }));

    const totalJobs = connections * jobsPerConn;
    const elapsed = Date.now() - start;
    clients.forEach(c => c.destroy());
    return { connections, jobsPerConn, totalJobs, elapsed, opsPerSec: Math.round(totalJobs / (elapsed / 1000)) };
}

async function benchmarkLatency(samples) {
    const client = await createConnection();
    const latencies = [];

    for (let i = 0; i < samples; i++) {
        const start = process.hrtime.bigint();
        await sendCommand(client, {
            cmd: 'PUSH',
            queue: 'bench5',
            data: { i }
        });
        const end = process.hrtime.bigint();
        latencies.push(Number(end - start) / 1000); // microseconds
    }

    latencies.sort((a, b) => a - b);
    client.destroy();

    return {
        samples,
        avg: Math.round(latencies.reduce((a, b) => a + b, 0) / latencies.length),
        min: Math.round(latencies[0]),
        max: Math.round(latencies[latencies.length - 1]),
        p50: Math.round(latencies[Math.floor(latencies.length * 0.5)]),
        p95: Math.round(latencies[Math.floor(latencies.length * 0.95)]),
        p99: Math.round(latencies[Math.floor(latencies.length * 0.99)])
    };
}

async function main() {
    console.log('MagicQueue Benchmark\n');
    console.log('='.repeat(50));

    // Warmup
    console.log('\nWarmup...');
    await benchmarkPush(1000);

    // Sequential Push
    console.log('\n[1] Sequential PUSH (10,000 jobs)');
    const push = await benchmarkPush(10000);
    console.log('    Ops/sec: ' + push.opsPerSec.toLocaleString());
    console.log('    Time: ' + push.elapsed + 'ms');

    // Push + Pull + ACK cycle
    console.log('\n[2] PUSH -> PULL -> ACK cycle (5,000 jobs)');
    const cycle = await benchmarkPushPull(5000);
    console.log('    Cycles/sec: ' + cycle.opsPerSec.toLocaleString());
    console.log('    Time: ' + cycle.elapsed + 'ms');

    // Batch Push
    console.log('\n[3] Batch PUSH (100 batches x 100 jobs = 10,000)');
    const batch = await benchmarkBatchPush(100, 100);
    console.log('    Ops/sec: ' + batch.opsPerSec.toLocaleString());
    console.log('    Time: ' + batch.elapsed + 'ms');

    // Concurrent connections
    console.log('\n[4] Concurrent PUSH (10 connections x 1,000 jobs)');
    const concurrent = await benchmarkConcurrent(10, 1000);
    console.log('    Ops/sec: ' + concurrent.opsPerSec.toLocaleString());
    console.log('    Time: ' + concurrent.elapsed + 'ms');

    // Latency
    console.log('\n[5] Latency (1,000 samples)');
    const latency = await benchmarkLatency(1000);
    console.log('    Avg: ' + latency.avg + 'us');
    console.log('    Min: ' + latency.min + 'us');
    console.log('    P50: ' + latency.p50 + 'us');
    console.log('    P95: ' + latency.p95 + 'us');
    console.log('    P99: ' + latency.p99 + 'us');
    console.log('    Max: ' + latency.max + 'us');

    console.log('\n' + '='.repeat(50));
    console.log('Benchmark complete!\n');

    // Output JSON for processing
    const results = {
        push: { ops: push.count, time: push.elapsed, throughput: push.opsPerSec },
        cycle: { ops: cycle.count, time: cycle.elapsed, throughput: cycle.opsPerSec },
        batch: { ops: batch.count, time: batch.elapsed, throughput: batch.opsPerSec },
        concurrent: { ops: concurrent.totalJobs, time: concurrent.elapsed, throughput: concurrent.opsPerSec },
        latency: latency
    };

    console.log('\n--- JSON RESULTS ---');
    console.log(JSON.stringify(results, null, 2));

    process.exit(0);
}

main().catch(console.error);
