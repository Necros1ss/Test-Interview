require('dotenv').config();
const amqp = require('amqplib');
const EventEmitter = require('events');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';

// Topology Exchange & Queue Names
const EXCHANGES = {
  ORDERS_TOPIC: 'orders.topic',
  NOTIFICATIONS_FANOUT: 'notifications.fanout',
  PAYMENT_DLX: 'payment.dlx'
};

const QUEUES = {
  PAYMENT: 'payment.queue',
  PAYMENT_RETRY: 'payment.retry.queue',
  PAYMENT_PARKING: 'payment.parking.queue',
  INVENTORY: 'inventory.queue',
  ORDER_STATUS_UPDATE: 'order.status.update.queue',
  EMAIL_NOTIFICATION: 'email.notification.queue',
  PUSH_NOTIFICATION: 'push.notification.queue',
  ANALYTICS: 'analytics.queue'
};

const ROUTING_KEYS = {
  ORDER_CREATED: 'order.created',
  INVENTORY_RESERVED: 'inventory.reserved',
  INVENTORY_FAILED: 'inventory.failed',
  PAYMENT_SUCCESS: 'payment.success',
  PAYMENT_FAILED: 'payment.failed',
  PAYMENT_RETRY_STEP: 'payment.retry.step',
  PAYMENT_RETRY_BACK: 'payment.retry.back',
  ORDER_CANCELLED: 'order.cancelled'
};

let connection = null;
let channel = null;
let useInMemoryFallback = false;
let isConnecting = false;

// In-Memory AMQP Broker Fallback for unit/integration tests without live RabbitMQ
class InMemoryBroker extends EventEmitter {
  constructor() {
    super();
    this.setMaxListeners(200);
  }

  publish(exchange, routingKey, payloadBuffer, options) {
    setImmediate(() => {
      const eventMsg = {
        content: payloadBuffer,
        fields: { routingKey, exchange },
        properties: {
          headers: options?.headers || {},
          messageId: options?.messageId,
          correlationId: options?.correlationId
        }
      };

      // Exact routing
      this.emit(`msg:${exchange}:${routingKey}`, eventMsg);
      // Wildcard / Prefix routing
      const prefix = routingKey.split('.')[0];
      this.emit(`msg:${exchange}:${prefix}.*`, eventMsg);
      // Fanout / Wildcard routing
      this.emit(`msg:${exchange}:*`, eventMsg);
      this.emit(`msg:${exchange}:#`, eventMsg);
    });
    return true;
  }
}

const memoryBroker = new InMemoryBroker();

async function connectRabbitMQ(retries = 2, delay = 1000) {
  if (channel) return { connection, channel, useInMemoryFallback };
  if (isConnecting) {
    while (isConnecting) {
      await new Promise((res) => setTimeout(res, 100));
    }
    if (channel) return { connection, channel, useInMemoryFallback };
  }

  isConnecting = true;

  for (let i = 0; i < retries; i++) {
    try {
      console.log(`[RabbitMQ] Attempting connection to ${RABBITMQ_URL}...`);
      connection = await amqp.connect(RABBITMQ_URL);

      connection.on('error', (err) => {
        console.error('[RabbitMQ Connection Error]:', err.message);
      });

      connection.on('close', () => {
        console.warn('[RabbitMQ Connection Closed]. Channel cleared.');
        channel = null;
        connection = null;
      });

      channel = await connection.createChannel();
      channel.on('error', (err) => {
        console.error('[RabbitMQ Channel Error]:', err.message);
      });

      console.log('[RabbitMQ] Connection established. Setting up topology...');
      await setupTopology(channel);
      useInMemoryFallback = false;
      isConnecting = false;
      return { connection, channel, useInMemoryFallback: false };
    } catch (err) {
      if (i === retries - 1) {
        console.warn(`[RabbitMQ] Live broker unavailable (${err.message}). Using test in-memory fallback.`);
        useInMemoryFallback = true;
        channel = createInMemoryChannel();
        isConnecting = false;
        return { connection: null, channel, useInMemoryFallback: true };
      }
      await new Promise((res) => setTimeout(res, delay));
    }
  }
}

function createInMemoryChannel() {
  return {
    prefetch: async () => {},
    assertExchange: async () => {},
    assertQueue: async () => {},
    bindQueue: async () => {},
    publish: (exchange, routingKey, payload, options) => {
      return memoryBroker.publish(exchange, routingKey, payload, options);
    },
    sendToQueue: (queueName, payload, options) => {
      memoryBroker.emit(`queue:${queueName}`, {
        content: payload,
        fields: { routingKey: queueName },
        properties: { headers: options?.headers || {}, correlationId: options?.correlationId }
      });
    },
    consume: (queueName, callback) => {
      if (queueName === QUEUES.INVENTORY) {
        // Inventory handles order.created (to reserve) & payment.failed (to release/compensate)
        memoryBroker.on(`msg:${EXCHANGES.ORDERS_TOPIC}:${ROUTING_KEYS.ORDER_CREATED}`, (msg) => callback(msg));
        memoryBroker.on(`msg:${EXCHANGES.ORDERS_TOPIC}:${ROUTING_KEYS.PAYMENT_FAILED}`, (msg) => callback(msg));
      } else if (queueName === QUEUES.PAYMENT) {
        // Payment processes ONLY after inventory is reserved
        memoryBroker.on(`msg:${EXCHANGES.ORDERS_TOPIC}:${ROUTING_KEYS.INVENTORY_RESERVED}`, (msg) => callback(msg));
        memoryBroker.on(`retry:${QUEUES.PAYMENT}`, (msg) => callback(msg));
      } else if (queueName === QUEUES.ORDER_STATUS_UPDATE) {
        // Order Status updates on payment success/failure and inventory failure
        memoryBroker.on(`msg:${EXCHANGES.ORDERS_TOPIC}:${ROUTING_KEYS.PAYMENT_SUCCESS}`, (msg) => callback(msg));
        memoryBroker.on(`msg:${EXCHANGES.ORDERS_TOPIC}:${ROUTING_KEYS.PAYMENT_FAILED}`, (msg) => callback(msg));
        memoryBroker.on(`msg:${EXCHANGES.ORDERS_TOPIC}:${ROUTING_KEYS.INVENTORY_FAILED}`, (msg) => callback(msg));
      } else if (queueName === QUEUES.EMAIL_NOTIFICATION || queueName === QUEUES.PUSH_NOTIFICATION) {
        memoryBroker.on(`msg:${EXCHANGES.NOTIFICATIONS_FANOUT}:*`, (msg) => callback(msg));
      } else if (queueName === QUEUES.ANALYTICS) {
        memoryBroker.on(`msg:${EXCHANGES.ORDERS_TOPIC}:#`, (msg) => callback(msg));
        memoryBroker.on(`msg:${EXCHANGES.ORDERS_TOPIC}:*`, (msg) => callback(msg));
      }
    },
    ack: () => {},
    nack: (msg, allUpTo, requeue) => {
      const orderData = JSON.parse(msg.content.toString());
      const currentCount = (msg.properties.headers['x-death'] ? msg.properties.headers['x-death'][0].count : 0) + 1;
      console.log(`[In-Memory DLQ] Retrying Order ${orderData.orderId} (Attempt ${currentCount}) in 3s...`);
      setTimeout(() => {
        msg.properties.headers['x-death'] = [{ count: currentCount }];
        memoryBroker.emit(`retry:${QUEUES.PAYMENT}`, msg);
      }, 3000);
    }
  };
}

async function setupTopology(ch) {
  // Exchanges
  await ch.assertExchange(EXCHANGES.ORDERS_TOPIC, 'topic', { durable: true });
  await ch.assertExchange(EXCHANGES.NOTIFICATIONS_FANOUT, 'fanout', { durable: true });
  await ch.assertExchange(EXCHANGES.PAYMENT_DLX, 'direct', { durable: true });

  // 1. Inventory Queue: Listens for order.created (step 1 of Saga) & payment.failed (compensation)
  await ch.assertQueue(QUEUES.INVENTORY, { durable: true });
  await ch.bindQueue(QUEUES.INVENTORY, EXCHANGES.ORDERS_TOPIC, ROUTING_KEYS.ORDER_CREATED);
  await ch.bindQueue(QUEUES.INVENTORY, EXCHANGES.ORDERS_TOPIC, ROUTING_KEYS.PAYMENT_FAILED);

  // 2. Payment Main Queue: Listens for inventory.reserved (step 2 of Saga) & DLX retries
  await ch.assertQueue(QUEUES.PAYMENT, {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': EXCHANGES.PAYMENT_DLX,
      'x-dead-letter-routing-key': ROUTING_KEYS.PAYMENT_RETRY_STEP
    }
  });
  await ch.bindQueue(QUEUES.PAYMENT, EXCHANGES.ORDERS_TOPIC, ROUTING_KEYS.INVENTORY_RESERVED);
  await ch.bindQueue(QUEUES.PAYMENT, EXCHANGES.PAYMENT_DLX, ROUTING_KEYS.PAYMENT_RETRY_BACK);

  // 3. Payment Retry Queue with TTL (3s): dead-letters back to payment.queue
  await ch.assertQueue(QUEUES.PAYMENT_RETRY, {
    durable: true,
    arguments: {
      'x-message-ttl': 3000,
      'x-dead-letter-exchange': EXCHANGES.PAYMENT_DLX,
      'x-dead-letter-routing-key': ROUTING_KEYS.PAYMENT_RETRY_BACK
    }
  });
  await ch.bindQueue(QUEUES.PAYMENT_RETRY, EXCHANGES.PAYMENT_DLX, ROUTING_KEYS.PAYMENT_RETRY_STEP);

  // 4. Payment Parking Queue for exhausted messages
  await ch.assertQueue(QUEUES.PAYMENT_PARKING, { durable: true });

  // 5. Order Status Update Queue: Listens for payment.* and inventory.failed
  await ch.assertQueue(QUEUES.ORDER_STATUS_UPDATE, { durable: true });
  await ch.bindQueue(QUEUES.ORDER_STATUS_UPDATE, EXCHANGES.ORDERS_TOPIC, 'payment.*');
  await ch.bindQueue(QUEUES.ORDER_STATUS_UPDATE, EXCHANGES.ORDERS_TOPIC, ROUTING_KEYS.INVENTORY_FAILED);

  // 6. Notification Queues (Fanout)
  await ch.assertQueue(QUEUES.EMAIL_NOTIFICATION, { durable: true });
  await ch.bindQueue(QUEUES.EMAIL_NOTIFICATION, EXCHANGES.NOTIFICATIONS_FANOUT, '');

  await ch.assertQueue(QUEUES.PUSH_NOTIFICATION, { durable: true });
  await ch.bindQueue(QUEUES.PUSH_NOTIFICATION, EXCHANGES.NOTIFICATIONS_FANOUT, '');

  // 7. Analytics Queue
  await ch.assertQueue(QUEUES.ANALYTICS, { durable: true });
  await ch.bindQueue(QUEUES.ANALYTICS, EXCHANGES.ORDERS_TOPIC, '#');
}

async function publishEvent(exchange, routingKey, data, options = {}) {
  if (!channel) throw new Error('[RabbitMQ] Channel not initialized.');
  const payload = Buffer.from(JSON.stringify(data));
  const correlationId = data.correlationId || options.correlationId || undefined;
  return channel.publish(exchange, routingKey, payload, {
    persistent: true,
    messageId: data.orderId || undefined,
    correlationId,
    headers: {
      'x-correlation-id': correlationId,
      ...(options.headers || {})
    },
    timestamp: Date.now()
  });
}

async function closeRabbitMQ() {
  if (channel) {
    try { await channel.close(); } catch (_) {}
    channel = null;
  }
  if (connection) {
    try { await connection.close(); } catch (_) {}
    connection = null;
  }
}

module.exports = {
  connectRabbitMQ,
  publishEvent,
  closeRabbitMQ,
  EXCHANGES,
  QUEUES,
  ROUTING_KEYS,
  memoryBroker
};
