import { kafka, ordersTopic, statusTopic } from './kafka.js'

const admin = kafka.admin()

try {
  await admin.connect()
  await admin.createTopics({
    waitForLeaders: true,
    topics: [ordersTopic, statusTopic].map(topic => ({
      topic, numPartitions: 1, replicationFactor: 1,
    })),
  })
  console.log(`Topics ready: ${ordersTopic}, ${statusTopic}`)
} finally {
  await admin.disconnect()
}
