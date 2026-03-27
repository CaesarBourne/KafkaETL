import { Kafka } from "kafkajs"

const kafka = new Kafka({
  clientId: "etl-admin",
  brokers: ["localhost:9092"]
})

const admin = kafka.admin()

async function createTopics() {
  await admin.connect()
  console.log("Connected to Kafka admin...")

  const existingTopics = await admin.listTopics()
  console.log("Existing topics:", existingTopics)

  const topicsToCreate = [
    { topic: "etl-topic", numPartitions: 1, replicationFactor: 1 },
    { topic: "etl-dlq", numPartitions: 1, replicationFactor: 1 }
  ]

  const topicsNeeded = topicsToCreate.filter(t => !existingTopics.includes(t.topic))

  if (topicsNeeded.length > 0) {
    await admin.createTopics({
      topics: topicsNeeded
    })
    console.log("Created topics:", topicsNeeded.map(t => t.topic))
  } else {
    console.log("All topics already exist")
  }

  await admin.disconnect()
  console.log("Setup complete!")
  process.exit(0)
}

createTopics().catch(err => {
  console.error("Failed to create topics:", err)
  process.exit(1)
})
