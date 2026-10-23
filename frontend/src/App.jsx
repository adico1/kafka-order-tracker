import {useEffect, useState} from 'react'
import { mergeOrders } from './orders.js'

export default function App() {
  const [product, setProduct] = useState('Coffee')
  const [quantity, setQuantity] = useState(1)
  const [orders, setOrders] = useState([])
  const [error, setError] = useState('')
  const [sending, setSending] = useState(false)
  const [connected, setConnected] = useState(false)

  useEffect(() => {
    const events = new EventSource('/api/events')
    events.onopen = () => setConnected(true)
    events.onerror = () => setConnected(false)
    events.addEventListener('orders', event => {
      setOrders(current => mergeOrders(current, JSON.parse(event.data)))
    })
    return () => events.close()
  }, [])

  async function placeOrder(event) {
    event.preventDefault()
    setError('')
    setSending(true)

    try {
      const response = await fetch('/api/orders', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({product, quantity})
      })
      const data = await response.json()
      if(!response.ok) {
        throw new Error(data.error ?? 'Could not place order')
      }

      setOrders(current => mergeOrders(current, [data]))
    } catch(err) {
      setError(err.message)
    } finally {
      setSending(false)
    }
  }
  return (
    <main style={{ maxWidth: 600, margin: '40px auto', padding: 20 }}>
      <h1>Kafka Coffee Shop</h1>
      <p role="status">{connected ? 'Live updates connected' : 'Connecting to live updates…'}</p>
      <form onSubmit={placeOrder}>
        <label>
          Product{' '}
          <select
            value={product}
            onChange={event => setProduct(event.target.value)}
          >
            <option>Coffee</option>
            <option>Tea</option>
            <option>Sandwich</option>
          </select>
        </label>
        <label>
          {' '}Quantity{' '}
          <input
            type="number"
            min="1"
            step="1"
            required
            value={quantity}
            onChange={event => setQuantity(Number(event.target.value))}
            style={{ width: 60 }}
            />
        </label>

        <button type="submit" disabled={sending}>
          {sending ? 'Sending ...' : 'Place Order'}
        </button>
      </form>
      {error && <p role="alert">{error}</p>}

      <h2>Orders</h2>
      {orders.length === 0 && <p>No orders yet.</p>}

      <ul>
        {orders.map(order => (
          <li key={order.orderId}>
            {order.quantity} x {order.product} - {order.status}
          </li>
        ))}
      </ul>
    </main>
  )
}
