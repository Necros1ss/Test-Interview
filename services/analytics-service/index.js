require('dotenv').config();
const express = require('express');
const { connectRabbitMQ, closeRabbitMQ, QUEUES } = require('../../common/rabbitmq');

const app = express();
app.use(express.json());

const PORT = process.env.ANALYTICS_SERVICE_PORT || 3005;

// Bounded event log (FIFO) to prevent memory leak
const MAX_EVENT_LOG_SIZE = 500;
const eventLog = [];
let totalEventsCount = 0;

async function trackEvent(eventData) {
  // Simulates BI aggregation / Database insert latency
  await new Promise((res) => setTimeout(res, 100));
  
  totalEventsCount++;
  if (eventLog.length >= MAX_EVENT_LOG_SIZE) {
    eventLog.shift(); // Remove oldest
  }
  eventLog.push({ ...eventData, receivedAt: new Date().toISOString() });
  console.log(`[Analytics Service] [Corr: ${eventData.correlationId || 'N/A'}] Aggregated Event: ${eventData.event || 'UNKNOWN'} | Order: ${eventData.orderId || 'N/A'}`);
}

function clearEvents() {
  eventLog.length = 0;
  totalEventsCount = 0;
}

// ----------------------------------------------------
// SYNCHRONOUS REST ENDPOINT
// ----------------------------------------------------
app.post('/track-sync', async (req, res) => {
  const correlationId = req.headers['x-correlation-id'] || req.body?.correlationId;
  try {
    await trackEvent({ ...(req.body || {}), correlationId });
    return res.json({ success: true, totalEvents: totalEventsCount, correlationId });
  } catch (err) {
    return res.status(500).json({ success: false, error: err.message, correlationId });
  }
});

// ----------------------------------------------------
// ASYNCHRONOUS RABBITMQ CONSUMER (TOPIC WILDCARD)
// ----------------------------------------------------
async function startAsyncConsumer() {
  try {
    const { channel } = await connectRabbitMQ();
    await channel.prefetch(50);

    console.log(`[Analytics Service - ASYNC] Listening on queue '${QUEUES.ANALYTICS}'...`);
    channel.consume(QUEUES.ANALYTICS, async (msg) => {
      if (!msg) return;
      try {
        const data = JSON.parse(msg.content.toString());
        const routingKey = msg.fields.routingKey;
        const correlationId = data.correlationId || msg.properties?.correlationId;

        await trackEvent({ event: routingKey, correlationId, ...data });
        channel.ack(msg);
      } catch (err) {
        console.error('[Analytics Service - ASYNC] Error processing message:', err.message);
        channel.ack(msg);
      }
    });
  } catch (err) {
    console.error('[Analytics Service - ASYNC] Consumer error:', err.message);
  }
}

app.get('/metrics', (req, res) => {
  return res.json({
    totalEventsTracked: totalEventsCount,
    recentEvents: eventLog.slice(-10)
  });
});

app.get('/health', (req, res) => {
  return res.json({ status: 'UP', service: 'Analytics Service' });
});

// Graceful Shutdown
async function shutdown() {
  console.log('[Analytics Service] Shutting down gracefully...');
  await closeRabbitMQ();
  process.exit(0);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

if (require.main === module) {
  app.listen(PORT, async () => {
    console.log(`[Analytics Service] HTTP server listening on port ${PORT}`);
    await startAsyncConsumer();
  });
}

module.exports = {
  app,
  startAsyncConsumer,
  trackEvent,
  clearEvents,
  eventLog,
  shutdown
};
