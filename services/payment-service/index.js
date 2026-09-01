require('dotenv').config();
const express = require('express');
const { initDb, query, closeDb } = require('../../common/db');
const { connectRabbitMQ, publishEvent, closeRabbitMQ, EXCHANGES, QUEUES, ROUTING_KEYS } = require('../../common/rabbitmq');

const app = express();
app.use(express.json());

const PORT = process.env.PAYMENT_SERVICE_PORT || 3002;
const SIMULATE_FAILURE_RATE = parseFloat(process.env.PAYMENT_FAILURE_RATE || '0.2'); // 20% simulated failure
const SIMULATE_LATENCY_MS = parseInt(process.env.PAYMENT_LATENCY_MS || '1000', 10); // 1.0s delay

let isRabbitReady = false;
let isDbReady = false;

// Helper to simulate payment gateway execution
async function executePayment(orderId, amount) {
  await new Promise((res) => setTimeout(res, SIMULATE_LATENCY_MS));

  const isFailure = Math.random() < SIMULATE_FAILURE_RATE;
  if (isFailure) {
    throw new Error(`[Payment Gateway] Connection timeout for order ${orderId}`);
  }
  return { transactionId: `TXN-${Date.now()}-${Math.floor(Math.random() * 1000)}`, status: 'SUCCESS' };
}

// ----------------------------------------------------
// SYNCHRONOUS REST ENDPOINT
// ----------------------------------------------------
app.post('/process-payment-sync', async (req, res) => {
  const { orderId, amount } = req.body || {};
  const correlationId = req.headers['x-correlation-id'] || req.body?.correlationId;

  try {
    const existing = await query(`SELECT * FROM payments WHERE order_id = $1`, [orderId]);
    if (existing.rows && existing.rows.length > 0) {
      return res.json({ success: true, correlationId, ...existing.rows[0] });
    }

    const result = await executePayment(orderId, amount);
    await query(
      `INSERT INTO payments (order_id, status, transaction_id, amount) VALUES ($1, $2, $3, $4)`,
      [orderId, 'SUCCESS', result.transactionId, parseFloat(amount || 150000)]
    );

    return res.json({ success: true, correlationId, ...result });
  } catch (err) {
    return res.status(500).json({ success: false, error: err.message, correlationId });
  }
});

// ----------------------------------------------------
// ASYNCHRONOUS RABBITMQ CONSUMER (Saga Step 2: Payment)
// ----------------------------------------------------
async function startAsyncConsumer() {
  try {
    await initDb();
    isDbReady = true;

    const { channel } = await connectRabbitMQ();
    isRabbitReady = true;
    await channel.prefetch(10);

    console.log(`[Payment Service - ASYNC] Listening on queue '${QUEUES.PAYMENT}'...`);

    channel.consume(QUEUES.PAYMENT, async (msg) => {
      if (!msg) return;

      let orderData;
      try {
        orderData = JSON.parse(msg.content.toString());
      } catch (parseErr) {
        console.error('[Payment Service] Malformed message body. Moving directly to parking queue:', parseErr.message);
        channel.sendToQueue(QUEUES.PAYMENT_PARKING, msg.content, { persistent: true });
        return channel.ack(msg);
      }

      const orderId = orderData.orderId;
      const correlationId = orderData.correlationId || msg.properties?.correlationId;
      const deathHeader = msg.properties.headers && msg.properties.headers['x-death'];
      const retryCount = deathHeader && deathHeader[0] ? deathHeader[0].count : 0;

      // 1. Persistent Idempotency Check
      const existingPay = await query(`SELECT * FROM payments WHERE order_id = $1`, [orderId]);
      if (existingPay.rows && existingPay.rows.length > 0 && existingPay.rows[0].status === 'SUCCESS') {
        console.log(`[Payment Service - ASYNC] Order ${orderId} already successfully paid. Skipping duplicate execution.`);
        return channel.ack(msg);
      }

      console.log(`[Payment Service - ASYNC] [Corr: ${correlationId || 'N/A'}] Processing payment for Order ${orderId} (Attempt ${retryCount + 1})`);

      try {
        const result = await executePayment(orderId, orderData.amount);

        // Record persistent payment
        await query(
          `INSERT INTO payments (order_id, status, transaction_id, amount) VALUES ($1, $2, $3, $4)
           ON CONFLICT (order_id) DO UPDATE SET status = 'SUCCESS', transaction_id = $3`,
          [orderId, 'SUCCESS', result.transactionId, parseFloat(orderData.amount || 150000)]
        );

        console.log(`[Payment Service - ASYNC] Payment SUCCESS for Order ${orderId} (Txn: ${result.transactionId})`);

        // SAGA STEP 3: Publish payment.success to transition Order to PAID
        await publishEvent(
          EXCHANGES.ORDERS_TOPIC,
          ROUTING_KEYS.PAYMENT_SUCCESS,
          {
            orderId,
            userId: orderData.userId,
            productId: orderData.productId,
            quantity: orderData.quantity,
            amount: orderData.amount,
            status: 'PAID',
            transactionId: result.transactionId,
            correlationId
          },
          { correlationId }
        );

        // Publish to Notification Fanout Exchange
        await publishEvent(
          EXCHANGES.NOTIFICATIONS_FANOUT,
          '',
          {
            type: 'PAYMENT_SUCCESS',
            orderId,
            userId: orderData.userId,
            amount: orderData.amount,
            transactionId: result.transactionId,
            correlationId
          },
          { correlationId }
        );

        channel.ack(msg);
      } catch (err) {
        console.error(`[Payment Service - ASYNC] Payment FAILED for Order ${orderId}: ${err.message}`);

        if (retryCount >= 3) {
          console.warn(`[Payment Service - ASYNC] Max retries (3) reached for Order ${orderId}. Moving to Parking Queue & triggering Saga Rollback.`);

          await query(
            `INSERT INTO payments (order_id, status, amount) VALUES ($1, $2, $3)
             ON CONFLICT (order_id) DO UPDATE SET status = 'FAILED'`,
            [orderId, 'FAILED', parseFloat(orderData.amount || 150000)]
          );

          // SAGA ROLLBACK: Publish payment.failed so Inventory releases stock and Order marks FAILED
          await publishEvent(
            EXCHANGES.ORDERS_TOPIC,
            ROUTING_KEYS.PAYMENT_FAILED,
            {
              orderId,
              productId: orderData.productId,
              quantity: orderData.quantity,
              userId: orderData.userId,
              status: 'FAILED',
              reason: err.message,
              correlationId
            },
            { correlationId }
          );

          await publishEvent(
            EXCHANGES.NOTIFICATIONS_FANOUT,
            '',
            {
              type: 'PAYMENT_FAILED',
              orderId,
              userId: orderData.userId,
              reason: err.message,
              correlationId
            },
            { correlationId }
          );

          // Move to parking queue
          channel.sendToQueue(QUEUES.PAYMENT_PARKING, msg.content, { persistent: true, correlationId });
          channel.ack(msg);
        } else {
          console.log(`[Payment Service - ASYNC] NACKing Order ${orderId} -> Moving to DLX Retry Queue (TTL 3s)`);
          channel.nack(msg, false, false);
        }
      }
    });
  } catch (err) {
    console.error('[Payment Service - ASYNC] Consumer error:', err.message);
  }
}

app.get('/health', (req, res) => {
  return res.json({ status: 'UP', service: 'Payment Service', isRabbitReady, isDbReady });
});

// Graceful Shutdown
async function shutdown() {
  console.log('[Payment Service] Shutting down gracefully...');
  await closeRabbitMQ();
  await closeDb();
  process.exit(0);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

if (require.main === module) {
  app.listen(PORT, async () => {
    console.log(`[Payment Service] HTTP server listening on port ${PORT}`);
    await startAsyncConsumer();
  });
}

module.exports = {
  app,
  startAsyncConsumer,
  executePayment,
  shutdown
};
