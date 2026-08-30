// Shim forwarding to orderRepository for backwards compatibility
const {
  createOrderInDb,
  updateOrderStatusInDb,
  getOrderFromDb,
  getAllOrdersFromDb
} = require('./orderRepository');

module.exports = {
  createOrder: createOrderInDb,
  updateOrderStatus: updateOrderStatusInDb,
  getOrder: getOrderFromDb,
  getAllOrders: getAllOrdersFromDb
};
