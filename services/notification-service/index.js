require('dotenv').config();
const express = require('express');
const { connectRabbitMQ, closeRabbitMQ, QUEUES } = require('../../common/rabbitmq');

const app = express();
app.use(express.json());

const PORT = process.env.NOTIFICATION_SERVICE_PORT || 3004;

async function sendEmailNotification(orderId, recipient, subject = 'Order Notification', correlationId) {
  await new Promise((res) => setTimeout(res, 250)); // Simulates 250ms SMTP email sending latency
  console.log(`[Notification Service - EMAIL] [Corr: ${correlationId || 'N/A'}] Sent '${subject}' to ${recipient || 'user@example.com'} for Order ${orderId}`);
}

async function sendPushNotification(orderId, recipient, message = 'Order Notification', correlationId) {
  await new Promise((res) => setTimeout(res, 150)); // Simulates 150ms Push Notification latency
  console.log(`[Notification Service - PUSH] [Corr: ${correlationId || 'N/A'}] Sent '${message}' to ${recipient || 'user'} for Order ${orderId}`);
}

// ----------------------------------------------------
// SYNCHRONOUS REST ENDPOINT
// ----------------------------------------------------
app.post('/notify-sync', async (req, res) => {
  const { orderId, userId } = req.body || {};
  const correlationId = req.headers['x-correlation-id'] || req.body?.correlationId;
  try {
    await sendEmailNotification(orderId, userId, 'Order Created', correlationId);
    await sendPushNotification(orderId, userId, 'Order Created', correlationId);
    return res.json({ success: true, correlationId });
  } catch (err) {
    return res.status(500).json({ success: false, error: err.message, correlationId });
  }
});

// ----------------------------------------------------
// ASYNCHRONOUS RABBITMQ CONSUMERS (FANOUT EXCHANGE)
// ----------------------------------------------------
async function startAsyncConsumers() {
  try {
    const { channel } = await connectRabbitMQ();
    await channel.prefetch(20);

    // Email Queue Consumer
    console.log(`[Notification Service - ASYNC] Listening on queue '${QUEUES.EMAIL_NOTIFICATION}'...`);
    channel.consume(QUEUES.EMAIL_NOTIFICATION, async (msg) => {
      if (!msg) return;
      try {
        const data = JSON.parse(msg.content.toString());
        const correlationId = data.correlationId || msg.properties?.correlationId;
        const subject = data.type === 'PAYMENT_FAILED' ? 'Payment Failed Alert' : 'Order Payment Confirmation';
        await sendEmailNotification(data.orderId, data.userId, subject, correlationId);
        channel.ack(msg);
      } catch (err) {
        console.error('[Notification Service - EMAIL] Error processing message:', err.message);
        channel.ack(msg);
      }
    });

    // Push Queue Consumer
    console.log(`[Notification Service - ASYNC] Listening on queue '${QUEUES.PUSH_NOTIFICATION}'...`);
    channel.consume(QUEUES.PUSH_NOTIFICATION, async (msg) => {
      if (!msg) return;
      try {
        const data = JSON.parse(msg.content.toString());
        const correlationId = data.correlationId || msg.properties?.correlationId;
        const notifText = data.type === 'PAYMENT_FAILED' ? 'Payment Failed for your order' : 'Payment Received!';
        await sendPushNotification(data.orderId, data.userId, notifText, correlationId);
        channel.ack(msg);
      } catch (err) {
        console.error('[Notification Service - PUSH] Error processing message:', err.message);
        channel.ack(msg);
      }
    });
  } catch (err) {
    console.error('[Notification Service - ASYNC] Consumer error:', err.message);
  }
}

app.get('/health', (req, res) => {
  return res.json({ status: 'UP', service: 'Notification Service' });
});

// Graceful Shutdown
async function shutdown() {
  console.log('[Notification Service] Shutting down gracefully...');
  await closeRabbitMQ();
  process.exit(0);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

if (require.main === module) {
  app.listen(PORT, async () => {
    console.log(`[Notification Service] HTTP server listening on port ${PORT}`);
    await startAsyncConsumers();
  });
}

module.exports = {
  app,
  startAsyncConsumers,
  sendEmailNotification,
  sendPushNotification,
  shutdown
};
