const test = require('node:test');
const assert = require('node:assert/strict');
const { initDb, query, resetDb } = require('../common/db');
const { connectRabbitMQ } = require('../common/rabbitmq');
const { saveOutboxEvent, dispatchPendingOutboxEvents } = require('../common/outbox');

test('Transactional Outbox Test Suite', async (t) => {
  t.before(async () => {
    await initDb();
    await connectRabbitMQ();
  });

  t.beforeEach(async () => {
    resetDb();
  });

  await t.test('1. saveOutboxEvent inserts record with status PENDING', async () => {
    const eventId = await saveOutboxEvent('Order', 'ORD-TEST-OUTBOX-1', 'order.created', {
      orderId: 'ORD-TEST-OUTBOX-1',
      amount: 250000
    });

    assert.ok(eventId.startsWith('EVT-'));

    const res = await query(`SELECT * FROM outbox_events WHERE id = $1`, [eventId]);
    assert.equal(res.rows.length, 1);
    assert.equal(res.rows[0].status, 'PENDING');
    assert.equal(res.rows[0].aggregate_id, 'ORD-TEST-OUTBOX-1');
  });

  await t.test('2. dispatchPendingOutboxEvents publishes pending events and marks as PUBLISHED', async () => {
    const eventId = await saveOutboxEvent('Order', 'ORD-TEST-OUTBOX-2', 'order.created', {
      orderId: 'ORD-TEST-OUTBOX-2',
      amount: 500000
    });

    await dispatchPendingOutboxEvents();

    const res = await query(`SELECT * FROM outbox_events WHERE id = $1`, [eventId]);
    assert.equal(res.rows.length, 1);
    assert.equal(res.rows[0].status, 'PUBLISHED');
  });

  await t.test('3. createOrderInDb executes atomically via withTransaction', async () => {
    const { createOrderInDb } = require('../services/order-service/orderRepository');
    const order = await createOrderInDb({
      orderId: 'ORD-TX-ATOMIC-01',
      userId: 'usr-tx',
      productId: 'prod-abc',
      quantity: 3,
      amount: 450000
    });

    assert.equal(order.orderId, 'ORD-TX-ATOMIC-01');

    const orderDb = await query(`SELECT * FROM orders WHERE id = $1`, ['ORD-TX-ATOMIC-01']);
    assert.equal(orderDb.rows.length, 1);

    const outboxDb = await query(`SELECT * FROM outbox_events WHERE aggregate_id = $1`, ['ORD-TX-ATOMIC-01']);
    assert.equal(outboxDb.rows.length, 1);
    assert.equal(outboxDb.rows[0].status, 'PENDING');
  });
});
