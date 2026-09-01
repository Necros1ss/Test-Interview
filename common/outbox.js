const { v4: uuidv4 } = require('uuid');
const { query } = require('./db');
const { publishEvent, EXCHANGES } = require('./rabbitmq');

let outboxTimer = null;
let isDispatching = false;

async function saveOutboxEvent(aggregateType, aggregateId, eventType, payload, dbClient = null) {
  const eventId = `EVT-${uuidv4()}`;
  const queryFn = dbClient ? (text, params) => dbClient.query(text, params) : query;
  await queryFn(
    `INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload, status)
     VALUES ($1, $2, $3, $4, $5, $6) RETURNING *`,
    [eventId, aggregateType, aggregateId, eventType, JSON.stringify(payload), 'PENDING']
  );
  return eventId;
}

async function dispatchPendingOutboxEvents() {
  if (isDispatching) return;
  isDispatching = true;

  try {
    const res = await query(
      `SELECT * FROM outbox_events WHERE status = $1 ORDER BY created_at ASC LIMIT 50`,
      ['PENDING']
    );

    if (res.rows && res.rows.length > 0) {
      for (const event of res.rows) {
        const payload = typeof event.payload === 'string' ? JSON.parse(event.payload) : event.payload;
        try {
          await publishEvent(
            EXCHANGES.ORDERS_TOPIC,
            event.event_type,
            payload,
            { correlationId: payload.correlationId }
          );

          await query(
            `UPDATE outbox_events SET status = $1 WHERE id = $2`,
            ['PUBLISHED', event.id]
          );
        } catch (pubErr) {
          console.error(`[Outbox Dispatcher] Failed to publish event ${event.id}:`, pubErr.message);
          break; // Retry in next interval
        }
      }
    }
  } catch (err) {
    // Suppress polling error if DB is in transition
  } finally {
    isDispatching = false;
  }
}

function startOutboxDispatcher(intervalMs = 300) {
  if (outboxTimer) return;
  outboxTimer = setInterval(dispatchPendingOutboxEvents, intervalMs);
  if (outboxTimer && outboxTimer.unref) outboxTimer.unref();
  console.log('[Outbox Dispatcher] Transactional outbox worker started.');
}

function stopOutboxDispatcher() {
  if (outboxTimer) {
    clearInterval(outboxTimer);
    outboxTimer = null;
  }
}

module.exports = {
  saveOutboxEvent,
  dispatchPendingOutboxEvents,
  startOutboxDispatcher,
  stopOutboxDispatcher
};
