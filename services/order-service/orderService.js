const {
  createOrderInDb,
  updateOrderStatusInDb,
  getOrderFromDb,
  getAllOrdersFromDb
} = require('./orderRepository');

// State Machine definition
const VALID_TRANSITIONS = {
  PENDING: ['PAID', 'FAILED', 'CANCELLED'],
  PAID: ['REFUNDED'],
  FAILED: [],
  CANCELLED: []
};

async function handleCreateOrder(orderData) {
  return createOrderInDb(orderData);
}

async function handleOrderStatusUpdate(orderId, nextStatus, details = {}) {
  const current = await getOrderFromDb(orderId);
  if (!current) {
    console.warn(`[Order Service] Order ${orderId} not found for status update.`);
    return null;
  }

  const allowed = VALID_TRANSITIONS[current.status] || [];
  if (!allowed.includes(nextStatus)) {
    console.warn(`[Order Service] Invalid state transition for Order ${orderId}: ${current.status} -> ${nextStatus}`);
    return current;
  }

  const updated = await updateOrderStatusInDb(orderId, nextStatus, {
    ...(current.details || {}),
    ...details,
    transitionedAt: new Date().toISOString()
  });

  console.log(`[Order Service] Order ${orderId} status transitioned: ${current.status} -> ${nextStatus}`);
  return updated;
}

module.exports = {
  handleCreateOrder,
  handleOrderStatusUpdate,
  getOrder: getOrderFromDb,
  getAllOrders: getAllOrdersFromDb
};
