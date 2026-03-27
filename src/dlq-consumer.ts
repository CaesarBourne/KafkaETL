import { Kafka } from "kafkajs"
import fs from "fs"

const kafka = new Kafka({
  clientId: "dlq-consumer",
  brokers: ["localhost:9092"]
})

const admin = kafka.admin()
const consumer = kafka.consumer({ groupId: "dlq-group" })

const dlqStream = fs.createWriteStream("output/dlq.json", { flags: "a" })

async function ensureTopicsExist() {
  await admin.connect()
  
  const existingTopics = await admin.listTopics()
  const topicsToCreate = ["etl-topic", "etl-dlq"].filter(t => !existingTopics.includes(t))
  
  if (topicsToCreate.length > 0) {
    await admin.createTopics({
      topics: topicsToCreate.map(topic => ({
        topic,
        numPartitions: 1,
        replicationFactor: 1
      }))
    })
    console.log("Created topics:", topicsToCreate)
  }
  
  await admin.disconnect()
}

async function run() {
  // Create topics first
  await ensureTopicsExist()

  await consumer.connect()

  await consumer.subscribe({ topic: "etl-dlq", fromBeginning: true })

  console.log("DLQ Consumer started, waiting for failed messages...")

  await consumer.run({
    eachMessage: async ({ message }) => {
      const raw = message.value?.toString()

      if (raw) {
        dlqStream.write(raw + "\n")
        console.log("DLQ received:", raw)
      }
    }
  })
}

run()