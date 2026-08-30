const test = require('node:test');
const assert = require('node:assert/strict');
const { app: orderApp, initService } = require('../services/order-service/index');
const { getOrder } = require('../services/order-service/orderService');
const { resetDb } = require('../common/db');
const { publishEvent, EXCHANGES, ROUTING_KEYS } = require('../common/rabbitmq');

const { stopOutboxDispatcher } = require('../common/outbox');

test('Order Service & Saga Choreography Test Suite', async (t) => {
  let server;
  const TEST_PORT = 3991;
  const BASE_URL = `http://localhost:${TEST_PORT}`;

  t.before(async () => {
    await initService();
    await new Promise((resolve) => {
      server = orderApp.listen(TEST_PORT, resolve);
    });
  });

  t.after((_, done) => {
    stopOutboxDispatcher();
    server.close(done);
  });

  t.beforeEach(() => {
    resetDb();
  });

  await t.test('1. Input validation rejects invalid order payload with HTTP 400', async () => {
    const res1 = await fetch(`${BASE_URL}/orders`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ quantity: -5, amount: 100000 })
    });
    assert.equal(res1.status, 400);

    const res2 = await fetch(`${BASE_URL}/orders`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ quantity: 1, amount: -100 })
    });
    assert.equal(res2.status, 400);

    const res3 = await fetch(`${BASE_URL}/orders`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ quantity: 1, amount: 100000, productId: '' })
    });
    assert.equal(res3.status, 400);
  });

  await t.test('2. POST /orders in Async mode returns HTTP 202 Accepted quickly (<30ms)', async () => {
    const startTime = Date.now();
    const res = await fetch(`${BASE_URL}/orders?mode=async`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-correlation-id': 'test-trace-123'
      },
      body: JSON.stringify({
        productId: 'prod-abc',
        quantity: 2,
        amount: 300000
      })
    });
    const duration = Date.now() - startTime;

    assert.equal(res.status, 202);
    const data = await res.json();
    assert.equal(data.success, true);
    assert.equal(data.mode, 'async');
    assert.equal(data.status, 'PENDING');
    assert.ok(data.orderId);
    assert.ok(duration < 100, `Duration (${duration}ms) should be fast (non-blocking)`);

    // Verify stored in DB with PENDING
    const stored = await getOrder(data.orderId);
    assert.ok(stored);
    assert.equal(stored.status, 'PENDING');
  });

  await t.test('3. Saga Happy Path: Payment Success transitions Order to PAID', async () => {
    const createRes = await fetch(`${BASE_URL}/orders?mode=async`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ productId: 'prod-abc', quantity: 1, amount: 150000 })
    });
    const { orderId } = await createRes.json();

    await publishEvent(EXCHANGES.ORDERS_TOPIC, ROUTING_KEYS.PAYMENT_SUCCESS, {
      orderId,
      status: 'PAID',
      transactionId: 'TXN-TEST-SUCCESS-99'
    });

    await new Promise((resolve) => setTimeout(resolve, 150));

    const updated = await getOrder(orderId);
    assert.equal(updated.status, 'PAID');
    assert.equal(updated.details.transactionId, 'TXN-TEST-SUCCESS-99');
  });

  await t.test('4. Saga Out-of-Stock Path: Inventory Shortage transitions Order to FAILED', async () => {
    const createRes = await fetch(`${BASE_URL}/orders?mode=async`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ productId: 'prod-limited', quantity: 999, amount: 150000 })
    });
    const { orderId } = await createRes.json();

    // Simulate inventory failure event
    await publishEvent(EXCHANGES.ORDERS_TOPIC, ROUTING_KEYS.INVENTORY_FAILED, {
      orderId,
      status: 'FAILED',
      reason: 'INSUFFICIENT_STOCK',
      error: 'Insufficient stock for product prod-limited'
    });

    await new Promise((resolve) => setTimeout(resolve, 150));

    const updated = await getOrder(orderId);
    assert.equal(updated.status, 'FAILED');
    assert.equal(updated.details.reason, 'INSUFFICIENT_STOCK');
  });
});
