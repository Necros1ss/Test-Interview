const test = require('node:test');
const assert = require('node:assert/strict');
const http = require('http');
const gatewayApp = require('../services/api-gateway/index');

test('API Gateway Test Suite', async (t) => {
  let server;
  const TEST_PORT = 3999;
  const BASE_URL = `http://localhost:${TEST_PORT}`;

  t.before((_, done) => {
    server = gatewayApp.listen(TEST_PORT, done);
  });

  t.after((_, done) => {
    server.close(done);
  });

  await t.test('1. Health Check endpoint is publicly accessible without auth', async () => {
    const res = await fetch(`${BASE_URL}/health`);
    assert.equal(res.status, 200);
    const data = await res.json();
    assert.equal(data.status, 'UP');
    assert.equal(data.service, 'API Gateway');
    assert.ok(res.headers.get('x-correlation-id'));
  });

  await t.test('2. Protected endpoints reject requests without token with HTTP 401', async () => {
    const res = await fetch(`${BASE_URL}/api/orders`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ productId: 'prod-abc', quantity: 1, amount: 150000 })
    });
    assert.equal(res.status, 401);
    const data = await res.json();
    assert.equal(data.error, 'Unauthorized');
  });

  await t.test('3. Protected endpoints reject invalid token with HTTP 401', async () => {
    const res = await fetch(`${BASE_URL}/api/orders`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer invalid-token-xyz'
      },
      body: JSON.stringify({ productId: 'prod-abc', quantity: 1, amount: 150000 })
    });
    assert.equal(res.status, 401);
  });

  await t.test('4. Accepts valid token via Bearer header or x-api-key', async () => {
    // When downstream is offline, gateway returns 502 Bad Gateway (meaning Auth passed!)
    const res = await fetch(`${BASE_URL}/api/orders`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer techlab-secret-token-2026'
      },
      body: JSON.stringify({ productId: 'prod-abc', quantity: 1, amount: 150000 })
    });
    // Should NOT be 401 Unauthorized
    assert.notEqual(res.status, 401);
  });

  await t.test('5. Injects Correlation ID and preserves existing Correlation ID', async () => {
    const customCorrId = 'custom-test-trace-999';
    const res = await fetch(`${BASE_URL}/health`, {
      headers: { 'x-correlation-id': customCorrId }
    });
    assert.equal(res.status, 200);
    assert.equal(res.headers.get('x-correlation-id'), customCorrId);
  });
});
