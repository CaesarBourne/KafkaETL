import fs from "fs"
import csv from "csv-parser"
import { Kafka } from "kafkajs"

const kafka = new Kafka({
  brokers: ["localhost:9092"]
})

const producer = kafka.producer()

const BATCH_SIZE = 100
let batch: any[] = []

async function run() {
  await producer.connect()

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