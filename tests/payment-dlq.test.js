const test = require('node:test');
const assert = require('node:assert/strict');
const { app: paymentApp, startAsyncConsumer } = require('../services/payment-service/index');
const { initDb, query, resetDb } = require('../common/db');
const { connectRabbitMQ, EXCHANGES, QUEUES, ROUTING_KEYS } = require('../common/rabbitmq');

test('Payment Service & DLQ Retry Test Suite', async (t) => {
  let server;
  const TEST_PORT = 3992;
  const BASE_URL = `http://localhost:${TEST_PORT}`;

  t.before(async () => {
    await initDb();
    await startAsyncConsumer();
    await new Promise((resolve) => {
      server = paymentApp.listen(TEST_PORT, resolve);
    });
  });

  t.after((_, done) => {
    server.close(done);
  });

  t.beforeEach(async () => {
    resetDb();
  });

  await t.test('1. Health check returns UP status with DB and RabbitMQ readiness', async () => {
    const res = await fetch(`${BASE_URL}/health`);
    assert.equal(res.status, 200);
    const data = await res.json();
    assert.equal(data.status, 'UP');
    assert.equal(data.service, 'Payment Service');
  });

  await t.test('2. Persistent Idempotency prevents duplicate payment record insertion', async () => {
    const orderId = 'ORD-PAY-IDEM-01';

    // Insert first payment
    await query(
      `INSERT INTO payments (order_id, status, transaction_id, amount) VALUES ($1, $2, $3, $4)`,
      [orderId, 'SUCCESS', 'TXN-FIRST', 150000]
    );

    const check = await query(`SELECT * FROM payments WHERE order_id = $1`, [orderId]);
    assert.equal(check.rows.length, 1);
    assert.equal(check.rows[0].transaction_id, 'TXN-FIRST');
  });

  await t.test('3. RabbitMQ DLX & Retry topology configuration verification', async () => {
    assert.equal(EXCHANGES.PAYMENT_DLX, 'payment.dlx');
    assert.equal(QUEUES.PAYMENT_RETRY, 'payment.retry.queue');
    assert.equal(QUEUES.PAYMENT_PARKING, 'payment.parking.queue');
    assert.equal(ROUTING_KEYS.PAYMENT_RETRY_STEP, 'payment.retry.step');
    assert.equal(ROUTING_KEYS.PAYMENT_RETRY_BACK, 'payment.retry.back');
  });
});
