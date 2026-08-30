require('dotenv').config();
const express = require('express');
const { initDb, closeDb } = require('../../common/db');
const { connectRabbitMQ, publishEvent, closeRabbitMQ, EXCHANGES, QUEUES, ROUTING_KEYS } = require('../../common/rabbitmq');
const { startOutboxDispatcher, stopOutboxDispatcher } = require('../../common/outbox');
const { handleCreateOrder, handleOrderStatusUpdate, getOrder, getAllOrders } = require('./orderService');

const app = express();
app.use(express.json());

const PORT = process.env.ORDER_SERVICE_PORT || 3001;
const PAYMENT_SERVICE_URL = process.env.PAYMENT_SERVICE_URL || 'http://localhost:3002';
const INVENTORY_SERVICE_URL = process.env.INVENTORY_SERVICE_URL || 'http://localhost:3003';
const NOTIFICATION_SERVICE_URL = process.env.NOTIFICATION_SERVICE_URL || 'http://localhost:3004';
const ANALYTICS_SERVICE_URL = process.env.ANALYTICS_SERVICE_URL || 'http://localhost:3005';

let isRabbitReady = false;
let isDbReady = false;

// Initialize dependencies
async function initService() {
  try {
    await initDb();
    isDbReady = true;

    const { channel } = await connectRabbitMQ();
    isRabbitReady = true;
    console.log('[Order Service] RabbitMQ connected & topology initialized.');

    startOutboxDispatcher(300);

    // Consumer to update Order Status asynchronously from Payment / Inventory events
    await channel.prefetch(20);
    channel.consume(QUEUES.ORDER_STATUS_UPDATE, async (msg) => {
      if (!msg) return;
      try {
        const eventData = JSON.parse(msg.content.toString());
        const routingKey = msg.fields.routingKey;
        const correlationId = eventData.correlationId || msg.properties?.correlationId;

        if (routingKey === ROUTING_KEYS.PAYMENT_SUCCESS || eventData.status === 'PAID') {
          await handleOrderStatusUpdate(eventData.orderId, 'PAID', {
            transactionId: eventData.transactionId,
            correlationId
          });
        } else if (routingKey === ROUTING_KEYS.INVENTORY_FAILED || eventData.reason === 'INSUFFICIENT_STOCK') {
          await handleOrderStatusUpdate(eventData.orderId, 'FAILED', {
            reason: 'INSUFFICIENT_STOCK',
            error: eventData.error || 'Out of stock',
            correlationId
          });
        } else if (routingKey === ROUTING_KEYS.PAYMENT_FAILED || eventData.status === 'FAILED') {
          await handleOrderStatusUpdate(eventData.orderId, 'FAILED', {
            reason: eventData.reason || 'PAYMENT_FAILED',
            correlationId
          });
        }
        channel.ack(msg);
      } catch (err) {
        console.error('[Order Service] Error processing status update:', err.message);
        channel.ack(msg);
      }
    });
  } catch (err) {
    console.warn('[Order Service] Dependency initialization warning:', err.message);
  }
}

// Input Validation Middleware
function validateOrderPayload(req, res, next) {
  const { quantity, amount, productId } = req.body || {};
  if (quantity !== undefined && (typeof quantity !== 'number' || quantity <= 0 || !Number.isInteger(quantity))) {
    return res.status(400).json({ error: 'Quantity must be a positive integer.' });
  }
  if (amount !== undefined && (typeof amount !== 'number' || amount <= 0)) {
    return res.status(400).json({ error: 'Amount must be a positive number.' });
  }
  if (productId !== undefined && (typeof productId !== 'string' || !productId.trim())) {
    return res.status(400).json({ error: 'ProductId must be a non-empty string.' });
  }
  next();
}

/**
 * POST /orders
 * Query Param / Header: mode=sync | mode=async
 */
app.post('/orders', validateOrderPayload, async (req, res) => {
  const startTime = Date.now();
  const mode = req.query.mode || req.headers['x-mode'] || (isRabbitReady ? 'async' : 'sync');
  const correlationId = req.headers['x-correlation-id'] || req.body?.correlationId || `corr-${Date.now()}`;

  const order = await handleCreateOrder({ ...req.body, correlationId });

  if (mode === 'sync') {
    // ----------------------------------------------------
    // SYNCHRONOUS HTTP MODE (Simulates legacy behavior)
    // ----------------------------------------------------
    try {
      // 1. Synchronous Inventory Call (Step 1 of Saga)
      const invRes = await fetch(`${INVENTORY_SERVICE_URL}/reserve-sync`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-correlation-id': correlationId },
        body: JSON.stringify({ orderId: order.orderId, productId: order.productId, quantity: order.quantity })
      });
      if (!invRes.ok) {
        throw new Error(`Inventory reservation failed with status ${invRes.status}`);
      }

      // 2. Synchronous Payment Call (Step 2 of Saga)
      const paymentRes = await fetch(`${PAYMENT_SERVICE_URL}/process-payment-sync`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-correlation-id': correlationId },
        body: JSON.stringify({ orderId: order.orderId, amount: order.amount })
      });
      if (!paymentRes.ok) {
        throw new Error(`Payment failed with status ${paymentRes.status}`);
      }

      // 3. Synchronous Notification Call
      await fetch(`${NOTIFICATION_SERVICE_URL}/notify-sync`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-correlation-id': correlationId },
        body: JSON.stringify({ orderId: order.orderId, userId: order.userId })
      });

      // 4. Synchronous Analytics Call
      await fetch(`${ANALYTICS_SERVICE_URL}/track-sync`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-correlation-id': correlationId },
        body: JSON.stringify({ event: 'order.created', orderId: order.orderId })
      });

      await handleOrderStatusUpdate(order.orderId, 'PAID');
      const durationMs = Date.now() - startTime;
      return res.status(200).json({
        success: true,
        mode: 'sync',
        durationMs,
        orderId: order.orderId,
        status: 'PAID',
        correlationId
      });
    } catch (err) {
      await handleOrderStatusUpdate(order.orderId, 'FAILED', { error: err.message });
      const durationMs = Date.now() - startTime;
      return res.status(504).json({
        success: false,
        mode: 'sync',
        durationMs,
        orderId: order.orderId,
        error: err.message,
        correlationId
      });
    }
  } else {
    // ----------------------------------------------------
    // ASYNCHRONOUS RABBITMQ MODE (Proposed solution with Outbox)
    // ----------------------------------------------------
    try {
      // Direct fast publish to RabbitMQ (Outbox worker also ensures delivery)
      await publishEvent(
        EXCHANGES.ORDERS_TOPIC,
        ROUTING_KEYS.ORDER_CREATED,
        {
          orderId: order.orderId,
          userId: order.userId,
          productId: order.productId,
          quantity: order.quantity,
          amount: order.amount,
          createdAt: order.createdTime,
          correlationId
        },
        { correlationId }
      );

      const durationMs = Date.now() - startTime;
      return res.status(202).json({
        success: true,
        mode: 'async',
        durationMs,
        orderId: order.orderId,
        status: 'PENDING',
        message: 'Order accepted for asynchronous processing',
        correlationId
      });
    } catch (err) {
      const durationMs = Date.now() - startTime;
      return res.status(500).json({
        success: false,
        mode: 'async',
        durationMs,
        orderId: order.orderId,
        error: err.message,
        correlationId
      });
    }
  }
});

app.get('/orders/:id', async (req, res) => {
  const order = await getOrder(req.params.id);
  if (!order) return res.status(404).json({ error: 'Order not found' });
  return res.json(order);
});

app.get('/orders', async (req, res) => {
  const limit = Math.min(100, parseInt(req.query.limit || '50', 10));
  const list = await getAllOrders(limit);
  return res.json(list);
});

app.get('/health', (req, res) => {
  return res.json({ status: 'UP', service: 'Order Service', isRabbitReady, isDbReady });
});

// Graceful Shutdown
async function shutdown() {
  console.log('[Order Service] Shutting down gracefully...');
  stopOutboxDispatcher();
  await closeRabbitMQ();
  await closeDb();
  process.exit(0);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

if (require.main === module) {
  app.listen(PORT, async () => {
    console.log(`[Order Service] Listening on port ${PORT}`);
    await initService();
  });
}

module.exports = {
  app,
  initService,
  initRabbitMQ: initService,
  shutdown
};
