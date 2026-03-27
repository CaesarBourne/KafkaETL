import { Kafka } from "kafkajs"
import fs from "fs"
import { transform } from "./transform"
import { insertTransaction } from "./db"

const kafka = new Kafka({
  brokers: ["localhost:9092"]
})

const consumer = kafka.consumer({ groupId: "etl-group" })
const producer = kafka.producer()

const outputStream = fs.createWriteStream("output/output.json", { flags: "a" })

async function processWithRetry(data: any, retries = 3): Promise<void> {
  try {
    const cleaned = transform(data)

    // write to file
    outputStream.write(JSON.stringify(cleaned) + "\n")

    // write to DB
    await insertTransaction(cleaned.userId, cleaned.amount)

  } catch (err) {
    if (retries > 0) {
      return processWithRetry(data, retries - 1)
    }
    throw err
  }
}

async function run() {
  await consumer.connect()
  await producer.connect()

  await consumer.subscribe({ topic: "etl-topic" })

  await consumer.run({
    eachMessage: async ({ message }) => {
      const raw = message.value?.toString()

      try {
        const data = JSON.parse(raw!)
        await processWithRetry(data)

        console.log("Processed:", data)

      } catch (err) {
        console.error("Failed → DLQ:", raw)

        await producer.send({
          topic: "etl-dlq",
          messages: [{ value: raw ?? null }]
        })
      }
    }
  })
}

run()