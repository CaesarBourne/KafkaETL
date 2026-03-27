import fs from "fs"
import csv from "csv-parser"
import { Kafka } from "kafkajs"

const kafka = new Kafka({
  clientId: "etl-producer",
  brokers: ["localhost:9092"]
})

const admin = kafka.admin()
const producer = kafka.producer()

const BATCH_SIZE = 100
let batch: any[] = []

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
  
  await producer.connect()
  console.log("Producer connected, reading from input.csv...")

  const stream = fs.createReadStream("data/input.csv").pipe(csv())

  stream.on("data", async (row) => {
    batch.push(row)

    if (batch.length >= BATCH_SIZE) {
      stream.pause()
      await sendBatch(batch)
      batch = []
      stream.resume()
    }
  })

  stream.on("end", async () => {
    if (batch.length > 0) {
      await sendBatch(batch)
    }

    console.log("Producer done")
    process.exit(0)
  })
}

async function sendBatch(batch: any[]) {
  await producer.send({
    topic: "etl-topic",
    messages: batch.map(row => ({
      value: JSON.stringify(row)
    }))
  })
}

run()