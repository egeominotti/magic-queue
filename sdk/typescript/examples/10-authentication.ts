/**
 * Authentication Example
 *
 * Demonstrates connecting with authentication tokens.
 *
 * Prerequisites:
 * - Start server with: AUTH_TOKENS=secret123,admin456 cargo run
 *
 * Run: npx ts-node examples/10-authentication.ts
 */

import { FlashQ } from '../src';

async function main() {
  console.log('═══════════════════════════════════════');
  console.log('🔐 AUTHENTICATION DEMO');
  console.log('═══════════════════════════════════════\n');

  // Test 1: Connection without token (should fail if auth is enabled)
  console.log('Test 1: Connecting without token...');
  const noAuthClient = new FlashQ({ host: 'localhost', port: 6789 });

  try {
    await noAuthClient.connect();

    // Try to push a job
    await noAuthClient.push('test', { message: 'no auth' });
    console.log('   ✅ Success (server has no auth enabled)\n');
    await noAuthClient.close();
  } catch (err) {
    console.log(`   ❌ Failed: ${(err as Error).message}`);
    console.log('   (This is expected if server requires authentication)\n');
  }

  // Test 2: Connection with valid token
  console.log('Test 2: Connecting with valid token...');
  const authClient = new FlashQ({
    host: 'localhost',
    port: 6789,
    token: 'secret123', // Must match AUTH_TOKENS on server
  });

  try {
    await authClient.connect();
    console.log('   ✅ Connected successfully');

    // Push a job
    const job = await authClient.push('auth-test', { message: 'authenticated!' });
    console.log(`   ✅ Pushed job ${job.id}`);

    // Pull and ack
    const pulled = await authClient.pull('auth-test');
    await authClient.ack(pulled.id);
    console.log(`   ✅ Pulled and acknowledged job ${pulled.id}\n`);

    await authClient.close();
  } catch (err) {
    console.log(`   ❌ Failed: ${(err as Error).message}\n`);
  }

  // Test 3: Connection with invalid token
  console.log('Test 3: Connecting with invalid token...');
  const badAuthClient = new FlashQ({
    host: 'localhost',
    port: 6789,
    token: 'wrong-token',
  });

  try {
    await badAuthClient.connect();
    console.log('   ⚠️  Connected (server may not have auth enabled)');
    await badAuthClient.close();
  } catch (err) {
    console.log(`   ❌ Failed: ${(err as Error).message}`);
    console.log('   (This is expected with invalid token)\n');
  }

  // Test 4: Late authentication
  console.log('Test 4: Late authentication (connect then auth)...');
  const lateAuthClient = new FlashQ({ host: 'localhost', port: 6789 });

  try {
    await lateAuthClient.connect();
    console.log('   Connected without token');

    // Authenticate after connecting
    await lateAuthClient.auth('secret123');
    console.log('   ✅ Authenticated successfully');

    // Now we can use the client
    const stats = await lateAuthClient.stats();
    console.log(`   ✅ Got stats: ${stats.queued} queued jobs\n`);

    await lateAuthClient.close();
  } catch (err) {
    console.log(`   ❌ Failed: ${(err as Error).message}\n`);
  }

  console.log('═══════════════════════════════════════');
  console.log('📝 Authentication Notes:');
  console.log('═══════════════════════════════════════');
  console.log(`
  1. Start server with authentication:
     AUTH_TOKENS=token1,token2 cargo run

  2. Multiple tokens can be used (comma-separated)

  3. Tokens are validated on first command or via AUTH

  4. WebSocket connections use ?token= query parameter:
     ws://localhost:6790/ws?token=secret123

  5. HTTP API can use Bearer token:
     Authorization: Bearer secret123
  `);

  console.log('👋 Done');
}

main().catch(console.error);
