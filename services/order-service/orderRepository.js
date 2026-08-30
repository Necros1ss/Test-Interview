const { query } = require('../../common/db');
const { saveOutboxEvent } = require('../../common/outbox');
const { ROUTING_KEYS } = require('../../common/rabbitmq');

async function createOrderInDb(orderData) {
  const orderId = orderData.orderId || `ORD-${Date.now()}-${Math.floor(Math.random() * 10000)}`;
  const userId = orderData.userId || 'user-123';
  const productId = orderData.productId || 'prod-abc';
  const quantity = Math.max(1, parseInt(orderData.quantity || 1, 10));
  const totalAmount = Math.max(0, parseFloat(orderData.amount || 150000));
  const correlationId = orderData.correlationId || null;
  const status = 'PENDING';

  // 1. Insert Order
  await query(
    `INSERT INTO orders (id, user_id, status, total_amount, correlation_id, details)
     VALUES ($1, $2, $3, $4, $5, $6) RETURNING *`,
    [orderId, userId, status, totalAmount, correlationId, JSON.stringify({ productId, quantity })]
  );

  // 2. Insert into Outbox for Transactional Outbox Pattern
  await saveOutboxEvent('Order', orderId, ROUTING_KEYS.ORDER_CREATED, {
    orderId,
    userId,
    productId,
    quantity,
    amount: totalAmount,
    correlationId,
    createdAt: new Date().toISOString()
  });

  return {
    orderId,
    userId,
    productId,
    quantity,
    amount: totalAmount,
    status,
    correlationId,
    createdTime: new Date().toISOString()
  };
}

async function updateOrderStatusInDb(orderId, status, details = {}) {
  const res = await query(
    `UPDATE orders SET status = $1, details = $2 WHERE id = $3 RETURNING *`,
    [status, JSON.stringify(details), orderId]
  );
  return res.rows && res.rows.length > 0 ? res.rows[0] : null;
}

async function getOrderFromDb(orderId) {
  const res = await query(`SELECT * FROM orders WHERE id = $1`, [orderId]);
  if (!res.rows || res.rows.length === 0) return null;
  const o = res.rows[0];
  return {
    orderId: o.id,
    userId: o.user_id,
    status: o.status,
    amount: parseFloat(o.total_amount),
    correlationId: o.correlation_id,
    details: typeof o.details === 'string' ? JSON.parse(o.details) : o.details,
    createdTime: o.created_at,
    updatedTime: o.updated_at
  };
}

async function getAllOrdersFromDb(limit = 100) {
  const res = await query(`SELECT * FROM orders ORDER BY created_at DESC LIMIT $1`, [limit]);
  return (res.rows || []).map((o) => ({
    orderId: o.id,
    userId: o.user_id,
    status: o.status,
    amount: parseFloat(o.total_amount),
    correlationId: o.correlation_id,
    details: typeof o.details === 'string' ? JSON.parse(o.details) : o.details,
    createdTime: o.created_at,
    updatedTime: o.updated_at
  }));
}

module.exports = {
  createOrderInDb,
  updateOrderStatusInDb,
  getOrderFromDb,
  getAllOrdersFromDb
};
