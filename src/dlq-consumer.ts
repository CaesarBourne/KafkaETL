import { Kafka } from "kafkajs"
import fs from "fs"

const kafka = new Kafka({
  brokers: ["localhost:9092"]
})

const consumer = kafka.consumer({ groupId: "dlq-group" })

const dlqStream = fs.createWriteStream("output/dlq.json", { flags: "a" })

async function run() {
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