import { setTimeout } from 'node:timers/promises'
import { Partitioners } from 'kafkajs'
import { kafka, ordersTopic, statusTopic } from './kafka.js'

const consumer = kafka.consumer({ groupId: 'kitchen-team' })
const producer = kafka.producer({
  createPartitioner: Partitioners.DefaultPartitioner,
  allowAutoTopicCreation: false,
})

await producer.connect()

async function publishStatus(order, status) {
  await producer.send({
    topic: statusTopic,
    messages: [{ key: order.orderId, value: JSON.stringify({ ...order, status }) }],
  })
}

await consumer.connect()
// A new group reads existing orders; returning groups resume committed offsets.
await consumer.subscribe({ topic: ordersTopic, fromBeginning: true })

await consumer.run({
  eachMessage: async ({ partition, message }) => {
    const order = JSON.parse(message.value.toString())
    await publishStatus(order, 'PREPARING')
    console.log('PREPARING', {
      ...order,
      status: 'PREPARING',
      partition,
      offset: message.offset,
    })

    // Simulate kitchen work without blocking Node's event loop.
    await setTimeout(2000)
    await publishStatus(order, 'READY')
    console.log('READY', { orderId: order.orderId })
  },
})

console.log(`Kitchen listening to ${ordersTopic} (group: kitchen-team)`)

let stopping = false
async function shutdown() {
  if (stopping) return
  stopping = true
  await consumer.disconnect()
  await producer.disconnect()
}

process.on('SIGINT', shutdown)
process.on('SIGTERM', shutdown)
