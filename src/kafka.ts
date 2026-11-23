import { Kafka, logLevel } from 'kafkajs'
import type { EventMessage } from './domain.ts'
import { consumeWithRetry, DLQ_TOPIC, TOPIC, type Publish } from './outbox.ts'

export function createKafka(brokers: string[]): Kafka {
  return new Kafka({ clientId: 'driveflow', brokers, logLevel: logLevel.ERROR })
}

export async function ensureTopics(kafka: Kafka): Promise<void> {
  const admin = kafka.admin()
  await admin.connect()
  try {
    await admin.createTopics({ topics: [{ topic: TOPIC, numPartitions: 3 }, { topic: DLQ_TOPIC }] })
  } finally {
    await admin.disconnect()
  }
}

export async function createPublisher(kafka: Kafka): Promise<{ publish: Publish; close: () => Promise<void> }> {
  const producer = kafka.producer({ idempotent: true, maxInFlightRequests: 1 })
  await producer.connect()
  return {
    publish: async (event) => {
      const message: EventMessage = {
        id: event._id,
        type: event.type,
        key: event.key,
        payload: event.payload,
        createdAt: event.createdAt.toISOString(),
      }
      await producer.send({
        topic: event.topic,
        acks: -1,
        messages: [{ key: event.key, value: JSON.stringify(message) }],
      })
    },
    close: () => producer.disconnect(),
  }
}

export async function runConsumer(
  kafka: Kafka,
  groupId: string,
  handle: (event: EventMessage) => Promise<unknown>,
): Promise<() => Promise<void>> {
  const consumer = kafka.consumer({ groupId })
  const deadLetters = kafka.producer()
  await Promise.all([consumer.connect(), deadLetters.connect()])
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
  await consumer.run({
    eachMessage: async ({ message }) => {
      const raw = message.value?.toString() ?? ''
      await consumeWithRetry(
        async () => handle(JSON.parse(raw)),
        async (error) => {
          await deadLetters.send({
            topic: DLQ_TOPIC,
            messages: [{ key: message.key, value: raw, headers: { error } }],
          })
        },
      )
    },
  })
  return async () => {
    await consumer.disconnect()
    await deadLetters.disconnect()
  }
}
