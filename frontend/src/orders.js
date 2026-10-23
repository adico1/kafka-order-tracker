const statusRank = { PLACED: 0, PREPARING: 1, READY: 2 }

// HTTP responses and SSE events can arrive in either order. Never regress a
// status or add the same order twice when a snapshot/event is replayed.
export function mergeOrders(current, incoming) {
  const orders = new Map(current.map(order => [order.orderId, order]))
  for (const order of incoming) {
    const previous = orders.get(order.orderId)
    if (!previous || statusRank[order.status] > statusRank[previous.status]) {
      orders.set(order.orderId, order)
    }
  }
  return [...orders.values()]
}
