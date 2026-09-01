const {
  createOrderInDb,
  updateOrderStatusInDb,
  getOrderFromDb,
  getAllOrdersFromDb
} = require('./orderRepository');

// Valid source states mapped to target state
const ALLOWED_SOURCE_STATES = {
  PAID: ['PENDING'],
  FAILED: ['PENDING'],
  CANCELLED: ['PENDING'],
  REFUNDED: ['PAID']
};

async function handleCreateOrder(orderData) {
  return createOrderInDb(orderData);
}

async function handleOrderStatusUpdate(orderId, nextStatus, details = {}) {
  const allowedSources = ALLOWED_SOURCE_STATES[nextStatus] || [];
  if (allowedSources.length === 0) {
    console.warn(`[Order Service] Unsupported target state transition: ${nextStatus}`);
    return null;
  }

  // Atomic conditional update in database to prevent Read-Modify-Write Race Condition
  const updated = await updateOrderStatusInDb(
    orderId,
    nextStatus,
    {
      ...details,
      transitionedAt: new Date().toISOString()
    },
    allowedSources
  );

  if (!updated) {
    const current = await getOrderFromDb(orderId);
    if (!current) {
      console.warn(`[Order Service] Order ${orderId} not found for status update.`);
      return null;
    }
    console.warn(`[Order Service] Order ${orderId} transition to ${nextStatus} skipped (Current status: ${current.status}).`);
    return current;
  }

  console.log(`[Order Service] Order ${orderId} status transitioned to: ${nextStatus}`);
  return updated;
}

module.exports = {
  handleCreateOrder,
  handleOrderStatusUpdate,
  getOrder: getOrderFromDb,
  getAllOrders: getAllOrdersFromDb
};
