import express from 'express'
import { randomUUID } from 'node:crypto'
import { Partitioners } from 'kafkajs'
import { kafka, ordersTopic, statusTopic } from './kafka.js'

const app = express()
// Reuse one producer for all HTTP requests.
const producer = kafka.producer({
  createPartitioner: Partitioners.DefaultPartitioner,
  allowAutoTopicCreation: false,
})
await producer.connect()

// A small in-memory view for this single-server training app.
const orders = new Map()
const streams = new Set()
const statusRank = { PLACED: 0, PREPARING: 1, READY: 2 }

function updateOrder(order) {
  const previous = orders.get(order.orderId)
  if (previous && statusRank[previous.status] >= statusRank[order.status]) return
  orders.set(order.orderId, order)
  for (const stream of streams) {
    stream.write(`event: orders\ndata: ${JSON.stringify([order])}\n\n`)
  }
}

const statusConsumer = kafka.consumer({ groupId: 'order-tracker-status-view' })
await statusConsumer.connect()
await statusConsumer.subscribe({ topic: statusTopic, fromBeginning: true })
await statusConsumer.run({
  // Replay retained status events after restart to rebuild the in-memory view.
  // A production service would persist its view and commit offsets.
  autoCommit: false,
  eachMessage: async ({ message }) => {
    updateOrder(JSON.parse(message.value.toString()))
  },
})

app.use(express.json())

app.get('/api/health', (req, res) => {
  res.json({ status: 'ok' })
})

app.get('/api/events', (req, res) => {
  res.set({
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'X-Accel-Buffering': 'no',
  })
  res.flushHeaders()
  streams.add(res)
  // A snapshot also catches updates missed while the browser was disconnected.
  res.write(`event: orders\ndata: ${JSON.stringify([...orders.values()])}\n\n`)
  const heartbeat = setInterval(() => res.write(': heartbeat\n\n'), 15000)
  res.on('close', () => {
    clearInterval(heartbeat)
    streams.delete(res)
  })
})

app.post('/api/orders', async (req, res) => {
  const menu = ['Coffee', 'Tea', 'Sandwich']
  const { product, quantity } = req.body ?? {}
  if(!menu.includes(product) || !Number.isInteger(quantity) || quantity < 1) {
    return res.status(400).json({
      error: 'Choose valid product and positive innteger quantity'
    })
  }

  const order = {
    orderId: randomUUID(),
    product,
    quantity,
    status: 'PLACED'
  }

  try {
    await producer.send({
      topic: ordersTopic,
      messages: [{ key: order.orderId, value: JSON.stringify(order) }],
    })
    console.log('Published order:', order)
    updateOrder(order)
    // Accepted for asynchronous processing, after Kafka acknowledges the send.
    res.status(202).json(orders.get(order.orderId))
  } catch (error) {
    console.error('Failed to publish order:', error)
    res.status(503).json({ error: 'Could not confirm order publication.' })
  }
})

const server = app.listen(3001, '127.0.0.1', () => {
  console.log('Backend running at http://127.0.0.1:3001')
})

let stopping = false
function shutdown() {
  if (stopping) return
  stopping = true
  for (const stream of streams) stream.end()
  server.close(async () => {
    await statusConsumer.disconnect()
    await producer.disconnect()
  })
}

process.on('SIGINT', shutdown)
process.on('SIGTERM', shutdown)
