const { Pool } = require('pg');

const DATABASE_URL = process.env.DATABASE_URL || 'postgresql://postgres:postgres@localhost:5432/techlab_db';

let pool = null;
let useMemoryStore = false;

// In-memory relational fallback for tests/offline development
class InMemoryRelationalStore {
  constructor() {
    this.orders = new Map();
    this.orderItems = [];
    this.inventory = new Map([
      ['prod-abc', 100000],
      ['prod-xyz', 500],
      ['prod-limited', 5]
    ]);
    this.reservations = new Map(); // key: `${order_id}:${product_id}`
    this.payments = new Map(); // key: order_id
    this.outbox = new Map(); // key: id
  }

  reset() {
    this.orders.clear();
    this.orderItems = [];
    this.inventory.set('prod-abc', 100000);
    this.inventory.set('prod-xyz', 500);
    this.inventory.set('prod-limited', 5);
    this.reservations.clear();
    this.payments.clear();
    this.outbox.clear();
  }
}

const memoryStore = new InMemoryRelationalStore();

async function initDb() {
  if (pool) return pool;

  try {
    const p = new Pool({
      connectionString: DATABASE_URL,
      connectionTimeoutMillis: 2000,
      max: 20
    });

    // Test connection
    const client = await p.connect();
    console.log('[PostgreSQL] Connected to database successfully.');

    // Execute schema migrations
    await client.query(`
      CREATE TABLE IF NOT EXISTS orders (
        id VARCHAR(64) PRIMARY KEY,
        user_id VARCHAR(64) NOT NULL,
        status VARCHAR(32) NOT NULL,
        total_amount NUMERIC(12,2) NOT NULL,
        correlation_id VARCHAR(128),
        details JSONB DEFAULT '{}',
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS order_items (
        id SERIAL PRIMARY KEY,
        order_id VARCHAR(64) REFERENCES orders(id) ON DELETE CASCADE,
        product_id VARCHAR(64) NOT NULL,
        quantity INT NOT NULL,
        price NUMERIC(12,2) NOT NULL
      );

      CREATE TABLE IF NOT EXISTS inventory (
        product_id VARCHAR(64) PRIMARY KEY,
        stock INT NOT NULL CHECK (stock >= 0),
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS inventory_reservations (
        id SERIAL PRIMARY KEY,
        order_id VARCHAR(64) NOT NULL,
        product_id VARCHAR(64) NOT NULL,
        quantity INT NOT NULL,
        status VARCHAR(32) NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (order_id, product_id)
      );

      CREATE TABLE IF NOT EXISTS payments (
        id SERIAL PRIMARY KEY,
        order_id VARCHAR(64) UNIQUE NOT NULL,
        status VARCHAR(32) NOT NULL,
        transaction_id VARCHAR(128),
        amount NUMERIC(12,2) NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS outbox_events (
        id VARCHAR(64) PRIMARY KEY,
        aggregate_type VARCHAR(64) NOT NULL,
        aggregate_id VARCHAR(64) NOT NULL,
        event_type VARCHAR(64) NOT NULL,
        payload JSONB NOT NULL,
        status VARCHAR(32) DEFAULT 'PENDING',
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        published_at TIMESTAMP WITH TIME ZONE
      );

      CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
      CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox_events(status);

      INSERT INTO inventory (product_id, stock) VALUES
        ('prod-abc', 100000),
        ('prod-xyz', 500),
        ('prod-limited', 5)
      ON CONFLICT (product_id) DO NOTHING;
    `);

    client.release();
    pool = p;
    useMemoryStore = false;
    return pool;
  } catch (err) {
    console.warn(`[PostgreSQL] Live DB unavailable (${err.message}). Using transactional in-memory store fallback.`);
    useMemoryStore = true;
    return null;
  }
}

async function query(text, params = []) {
  if (!useMemoryStore && pool) {
    return pool.query(text, params);
  }

  // Handle in-memory operations
  const sql = text.trim();

  // 1. SELECT stock FROM inventory WHERE product_id = $1
  if (sql.startsWith('SELECT stock FROM inventory') || sql.includes('FROM inventory WHERE product_id')) {
    const prodId = params[0];
    const stock = memoryStore.inventory.get(prodId);
    if (stock === undefined) return { rows: [], rowCount: 0 };
    return { rows: [{ product_id: prodId, stock }], rowCount: 1 };
  }

  // 2. Atomic stock decrement: UPDATE inventory SET stock = stock - $1 WHERE product_id = $2 AND stock >= $1
  if (sql.includes('UPDATE inventory') && sql.includes('stock = stock -')) {
    const qty = params[0];
    const prodId = params[1];
    const current = memoryStore.inventory.get(prodId);
    if (current !== undefined && current >= qty) {
      const updated = current - qty;
      memoryStore.inventory.set(prodId, updated);
      return { rows: [{ product_id: prodId, stock: updated }], rowCount: 1 };
    }
    return { rows: [], rowCount: 0 };
  }

  // 3. Stock release: UPDATE inventory SET stock = stock + $1 WHERE product_id = $2
  if (sql.includes('UPDATE inventory') && sql.includes('stock = stock +')) {
    const qty = params[0];
    const prodId = params[1];
    const current = memoryStore.inventory.get(prodId) || 0;
    const updated = current + qty;
    memoryStore.inventory.set(prodId, updated);
    return { rows: [{ product_id: prodId, stock: updated }], rowCount: 1 };
  }

  // 4. INSERT INTO inventory_reservations
  if (sql.includes('INSERT INTO inventory_reservations')) {
    const [orderId, prodId, qty, status] = params;
    const key = `${orderId}:${prodId}`;
    if (memoryStore.reservations.has(key)) {
      const err = new Error('duplicate key value violates unique constraint "inventory_reservations_order_id_product_id_key"');
      err.code = '23505';
      throw err;
    }
    memoryStore.reservations.set(key, { orderId, prodId, qty, status });
    return { rowCount: 1 };
  }

  // 5. UPDATE inventory_reservations SET status = $1 WHERE order_id = $2
  if (sql.includes('UPDATE inventory_reservations SET status')) {
    const [status, orderId] = params;
    let found = null;
    for (const [k, v] of memoryStore.reservations.entries()) {
      if (v.orderId === orderId) {
        v.status = status;
        found = v;
      }
    }
    return { rows: found ? [found] : [], rowCount: found ? 1 : 0 };
  }

  // 6. SELECT * FROM inventory_reservations WHERE order_id = $1
  if (sql.includes('FROM inventory_reservations WHERE order_id')) {
    const orderId = params[0];
    const results = [];
    for (const v of memoryStore.reservations.values()) {
      if (v.orderId === orderId) results.push(v);
    }
    return { rows: results, rowCount: results.length };
  }

  // 7. INSERT INTO orders
  if (sql.includes('INSERT INTO orders')) {
    const [id, userId, status, totalAmount, correlationId, details] = params;
    const order = {
      id,
      user_id: userId,
      status,
      total_amount: parseFloat(totalAmount),
      correlation_id: correlationId,
      details: details ? (typeof details === 'string' ? JSON.parse(details) : details) : {},
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    };
    memoryStore.orders.set(id, order);
    return { rows: [order], rowCount: 1 };
  }

  // 8. UPDATE orders SET status = $1
  if (sql.includes('UPDATE orders SET status')) {
    const [status, details, orderId] = params;
    const order = memoryStore.orders.get(orderId);
    if (order) {
      order.status = status;
      if (details) {
        order.details = { ...(order.details || {}), ...(typeof details === 'string' ? JSON.parse(details) : details) };
      }
      order.updated_at = new Date().toISOString();
      return { rows: [order], rowCount: 1 };
    }
    return { rows: [], rowCount: 0 };
  }

  // 9. SELECT * FROM orders WHERE id = $1
  if (sql.includes('FROM orders WHERE id')) {
    const id = params[0];
    const order = memoryStore.orders.get(id);
    return { rows: order ? [order] : [], rowCount: order ? 1 : 0 };
  }

  // 10. SELECT * FROM orders
  if (sql.includes('FROM orders')) {
    const list = Array.from(memoryStore.orders.values()).slice(-100);
    return { rows: list, rowCount: list.length };
  }

  // 11. Payments table
  if (sql.includes('INSERT INTO payments')) {
    const [orderId, status, txnId, amount] = params;
    if (memoryStore.payments.has(orderId)) {
      const err = new Error('duplicate key value violates unique constraint "payments_order_id_key"');
      err.code = '23505';
      throw err;
    }
    const rec = { order_id: orderId, status, transaction_id: txnId, amount: parseFloat(amount) };
    memoryStore.payments.set(orderId, rec);
    return { rows: [rec], rowCount: 1 };
  }

  if (sql.includes('FROM payments WHERE order_id')) {
    const orderId = params[0];
    const rec = memoryStore.payments.get(orderId);
    return { rows: rec ? [rec] : [], rowCount: rec ? 1 : 0 };
  }

  // 12. Outbox table
  if (sql.includes('INSERT INTO outbox_events')) {
    const [id, aggType, aggId, eventType, payload, status] = params;
    const evt = {
      id,
      aggregate_type: aggType,
      aggregate_id: aggId,
      event_type: eventType,
      payload: typeof payload === 'string' ? JSON.parse(payload) : payload,
      status: status || 'PENDING',
      created_at: new Date().toISOString()
    };
    memoryStore.outbox.set(id, evt);
    return { rows: [evt], rowCount: 1 };
  }

  if (sql.includes('UPDATE outbox_events SET status')) {
    const [status, id] = params;
    const evt = memoryStore.outbox.get(id);
    if (evt) {
      evt.status = status;
      evt.published_at = new Date().toISOString();
      return { rows: [evt], rowCount: 1 };
    }
    return { rows: [], rowCount: 0 };
  }

  if (sql.includes('FROM outbox_events WHERE id =') || sql.includes('FROM outbox_events WHERE id=')) {
    const id = params[0];
    const evt = memoryStore.outbox.get(id);
    return { rows: evt ? [evt] : [], rowCount: evt ? 1 : 0 };
  }

  if (sql.includes('FROM outbox_events WHERE status =') || sql.includes('FROM outbox_events WHERE status=')) {
    const status = params[0] || 'PENDING';
    const pending = Array.from(memoryStore.outbox.values()).filter((e) => e.status === status);
    return { rows: pending, rowCount: pending.length };
  }

  return { rows: [], rowCount: 0 };
}

async function closeDb() {
  if (pool) {
    await pool.end();
    pool = null;
  }
}

function resetDb() {
  memoryStore.reset();
}

module.exports = {
  initDb,
  query,
  closeDb,
  resetDb,
  memoryStore
};
