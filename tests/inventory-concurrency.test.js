const test = require('node:test');
const assert = require('node:assert/strict');
const { reserveStock, releaseStock } = require('../services/inventory-service/index');
const { initDb, query, resetDb } = require('../common/db');

test('Inventory Concurrency & Atomic Database Reservation Test Suite', async (t) => {
  t.before(async () => {
    await initDb();
  });

  t.beforeEach(async () => {
    resetDb();
  });

  await t.test('1. Normal single reservation and stock release (Compensation)', async () => {
    const orderId = 'ORD-SINGLE-01';
    const remaining = await reserveStock(orderId, 'prod-xyz', 3);
    assert.equal(remaining, 497, 'Remaining stock should be 497 after reserving 3 items from 500');

    const stockAfterRelease = await releaseStock(orderId, 'prod-xyz', 3);
    assert.equal(stockAfterRelease, 500, 'Stock should return to 500 after compensation release');
  });

  await t.test('2. Idempotency: Duplicate reserve call with same orderId does not deduct twice', async () => {
    const orderId = 'ORD-IDEM-01';
    const firstReserve = await reserveStock(orderId, 'prod-xyz', 2);
    assert.equal(firstReserve, 498);

    // Duplicate call
    const secondReserve = await reserveStock(orderId, 'prod-xyz', 2);
    assert.equal(secondReserve, 498, 'Second call should be skipped and stock must remain 498');
  });

  await t.test('3. High Concurrency Race Condition Prevention (20 concurrent requests for 5 items)', async () => {
    const totalRequests = 20;
    const initialStock = 5;

    // Set stock to 5
    await query(`UPDATE inventory SET stock = $1 WHERE product_id = $2`, [initialStock, 'prod-limited']);

    const results = await Promise.allSettled(
      Array.from({ length: totalRequests }, (_, i) => {
        const orderId = `CONC-ORD-${i}`;
        return reserveStock(orderId, 'prod-limited', 1);
      })
    );

    const fulfilled = results.filter((r) => r.status === 'fulfilled');
    const rejected = results.filter((r) => r.status === 'rejected');

    assert.equal(fulfilled.length, 5, 'Exactly 5 requests must succeed');
    assert.equal(rejected.length, 15, 'Exactly 15 requests must fail due to stock shortage');

    const stockRes = await query(`SELECT stock FROM inventory WHERE product_id = $1`, ['prod-limited']);
    assert.equal(stockRes.rows[0].stock, 0, 'Final stock must be exactly 0, never negative');
  });

  await t.test('4. Insufficient stock throws descriptive Error', async () => {
    await assert.rejects(
      async () => {
        await reserveStock('ORD-FAIL-01', 'prod-limited', 999);
      },
      {
        name: 'Error',
        message: /Insufficient stock/
      }
    );
  });
});
