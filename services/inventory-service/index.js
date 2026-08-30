require('dotenv').config();
const express = require('express');
const { initDb, query, closeDb } = require('../../common/db');
const { connectRabbitMQ, publishEvent, closeRabbitMQ, EXCHANGES, QUEUES, ROUTING_KEYS } = require('../../common/rabbitmq');

const app = express();
app.use(express.json());

const PORT = process.env.INVENTORY_SERVICE_PORT || 3003;

let isRabbitReady = false;
let isDbReady = false;

// ----------------------------------------------------
// ATOMIC INVENTORY OPERATIONS (PostgreSQL / Concurrency Safe)
// ----------------------------------------------------

async function reserveStock(orderId, productId, quantity) {
  const targetQty = Math.max(1, parseInt(quantity || 1, 10));
  const targetProduct = productId || 'prod-abc';

  // 1. Idempotency Check: Check if reservation already exists
  const existing = await query(
    `SELECT * FROM inventory_reservations WHERE order_id = $1 AND product_id = $2`,
    [orderId, targetProduct]
  );
  if (existing.rows && existing.rows.length > 0) {
    const res = existing.rows[0];
    console.log(`[Inventory Service] Order ${orderId} already reserved stock for ${targetProduct}. Skipping duplicate.`);
    const stockRes = await query(`SELECT stock FROM inventory WHERE product_id = $1`, [targetProduct]);
    return stockRes.rows[0]?.stock || 0;
  }

  // 2. Atomic Decrement: Only updates if current stock >= quantity
  const updateRes = await query(
    `UPDATE inventory SET stock = stock - $1, updated_at = CURRENT_TIMESTAMP
     WHERE product_id = $2 AND stock >= $1 RETURNING stock`,
    [targetQty, targetProduct]
  );

  if (!updateRes.rows || updateRes.rows.length === 0) {
    throw new Error(`Insufficient stock for product ${targetProduct}. Requested: ${targetQty}`);
  }

  const remainingStock = updateRes.rows[0].stock;

  // 3. Record reservation for idempotency and compensation
  try {
    await query(
      `INSERT INTO inventory_reservations (order_id, product_id, quantity, status)
       VALUES ($1, $2, $3, $4)`,
      [orderId, targetProduct, targetQty, 'RESERVED']
    );
  } catch (dupErr) {
    // If concurrent duplicate insert occurred, rollback stock
    await query(
      `UPDATE inventory SET stock = stock + $1 WHERE product_id = $2`,
      [targetQty, targetProduct]
    );
    throw dupErr;
  }

  return remainingStock;
}

async function releaseStock(orderId, productId, quantity) {
  const targetProduct = productId || 'prod-abc';

  const reservationRes = await query(
    `SELECT * FROM inventory_reservations WHERE order_id = $1 AND product_id = $2`,
    [orderId, targetProduct]
  );

  if (!reservationRes.rows || reservationRes.rows.length === 0 || reservationRes.rows[0].status === 'RELEASED') {
    console.log(`[Inventory Service - Compensation] No active reservation to release for Order ${orderId}.`);
    const stockRes = await query(`SELECT stock FROM inventory WHERE product_id = $1`, [targetProduct]);
    return stockRes.rows[0]?.stock || 0;
  }

  const releaseQty = quantity || reservationRes.rows[0].quantity || 1;

  // 1. Atomic Increment
  const updateRes = await query(
    `UPDATE inventory SET stock = stock + $1, updated_at = CURRENT_TIMESTAMP
     WHERE product_id = $2 RETURNING stock`,
    [releaseQty, targetProduct]
  );

  // 2. Mark reservation as RELEASED
  await query(
    `UPDATE inventory_reservations SET status = $1 WHERE order_id = $2 AND product_id = $3`,
    ['RELEASED', orderId, targetProduct]
  );

  const newStock = updateRes.rows[0]?.stock || 0;
  console.log(`[Inventory Service - Compensation] Released ${releaseQty} item(s) for Order ${orderId}. New stock: ${newStock}`);
  return newStock;
}

// ----------------------------------------------------
// SYNCHRONOUS REST ENDPOINTS
// ----------------------------------------------------
app.post('/reserve-sync', async (req, res) => {
  const { productId, quantity, orderId } = req.body || {};
  try {
    const targetProduct = productId || 'prod-abc';
    const targetQuantity = Math.max(1, parseInt(quantity || 1, 10));
    const targetOrderId = orderId || `SYNC-ORD-${Date.now()}`;

    const remainingStock = await reserveStock(targetOrderId, targetProduct, targetQuantity);
    return res.json({ success: true, remainingStock });
  } catch (err) {
    return res.status(400).json({ success: false, error: err.message });
  }
});

app.post('/release-sync', async (req, res) => {
  const { productId, quantity, orderId } = req.body || {};
  try {
    const remainingStock = await releaseStock(orderId, productId, quantity);
    return res.json({ success: true, remainingStock });
  } catch (err) {
    return res.status(400).json({ success: false, error: err.message });
  }
});

// ----------------------------------------------------
// ASYNCHRONOUS RABBITMQ CONSUMER (Saga Orchestration Step 1)
// ----------------------------------------------------
async function startAsyncConsumer() {
  try {
    await initDb();
    isDbReady = true;

    const { channel } = await connectRabbitMQ();
    isRabbitReady = true;
    await channel.prefetch(20);

    console.log(`[Inventory Service - ASYNC] Listening on queue '${QUEUES.INVENTORY}'...`);

    channel.consume(QUEUES.INVENTORY, async (msg) => {
      if (!msg) return;

      let data;
      try {
        data = JSON.parse(msg.content.toString());
      } catch (parseErr) {
        console.error('[Inventory Service] Malformed message. Rejecting:', parseErr.message);
        return channel.nack(msg, false, false);
      }

      const routingKey = msg.fields.routingKey;
      const correlationId = data.correlationId || msg.properties?.correlationId;

      try {
        if (routingKey === ROUTING_KEYS.PAYMENT_FAILED || data.status === 'FAILED') {
          // Saga Compensation: Release reserved stock
          console.log(`[Inventory Service - SAGA COMPENSATION] Received payment.failed for Order ${data.orderId}. Releasing stock...`);
          await releaseStock(data.orderId, data.productId || 'prod-abc', data.quantity || 1);
        } else {
          // Step 1 of Saga: Reserve stock on order.created
          try {
            const remainingStock = await reserveStock(data.orderId, data.productId || 'prod-abc', data.quantity || 1);
            console.log(`[Inventory Service - ASYNC] Reserved stock for Order ${data.orderId}. Remaining: ${remainingStock}`);

            // SAGA SUCCESS: Publish inventory.reserved to trigger Payment Service
            await publishEvent(
              EXCHANGES.ORDERS_TOPIC,
              ROUTING_KEYS.INVENTORY_RESERVED,
              {
                orderId: data.orderId,
                userId: data.userId,
                productId: data.productId,
                quantity: data.quantity,
                amount: data.amount,
                remainingStock,
                correlationId
              },
              { correlationId }
            );
          } catch (reserveErr) {
            console.warn(`[Inventory Service - SAGA DEFECT FIX] Insufficient stock for Order ${data.orderId}: ${reserveErr.message}`);
            // SAGA FAILURE: Publish inventory.failed to update Order to FAILED (Payment is NOT triggered)
            await publishEvent(
              EXCHANGES.ORDERS_TOPIC,
              ROUTING_KEYS.INVENTORY_FAILED,
              {
                orderId: data.orderId,
                productId: data.productId,
                quantity: data.quantity,
                status: 'FAILED',
                reason: 'INSUFFICIENT_STOCK',
                error: reserveErr.message,
                correlationId
              },
              { correlationId }
            );
          }
        }
        channel.ack(msg);
      } catch (err) {
        console.error(`[Inventory Service - ASYNC] Stock operation error for Order ${data.orderId}:`, err.message);
        channel.ack(msg);
      }
    });
  } catch (err) {
    console.error('[Inventory Service - ASYNC] Consumer initialization error:', err.message);
  }
}

app.get('/stock/:productId', async (req, res) => {
  const stockRes = await query(`SELECT stock FROM inventory WHERE product_id = $1`, [req.params.productId]);
  const stock = stockRes.rows && stockRes.rows.length > 0 ? stockRes.rows[0].stock : 0;
  return res.json({ productId: req.params.productId, stock });
});

app.put('/stock/:productId', async (req, res) => {
  const newStock = parseInt(req.body.stock, 10);
  if (isNaN(newStock) || newStock < 0) {
    return res.status(400).json({ error: 'Stock must be a non-negative integer' });
  }
  await query(
    `INSERT INTO inventory (product_id, stock) VALUES ($1, $2)
     ON CONFLICT (product_id) DO UPDATE SET stock = $2, updated_at = CURRENT_TIMESTAMP`,
    [req.params.productId, newStock]
  );
  return res.json({ productId: req.params.productId, stock: newStock });
});

app.get('/health', (req, res) => {
  return res.json({ status: 'UP', service: 'Inventory Service', isRabbitReady, isDbReady });
});

// Graceful Shutdown
async function shutdown() {
  console.log('[Inventory Service] Shutting down gracefully...');
  await closeRabbitMQ();
  await closeDb();
  process.exit(0);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

if (require.main === module) {
  app.listen(PORT, async () => {
    console.log(`[Inventory Service] HTTP server listening on port ${PORT}`);
    await startAsyncConsumer();
  });
}

module.exports = {
  app,
  startAsyncConsumer,
  reserveStock,
  releaseStock,
  shutdown
};
