import { Kafka } from 'kafkajs'

export const ordersTopic = 'orders.created'
export const statusTopic = 'orders.status'

export const kafka = new Kafka({
  clientId: 'order-tracker',
  brokers: ['localhost:9092'],
})
