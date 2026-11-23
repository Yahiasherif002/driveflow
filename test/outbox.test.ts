import { randomUUID } from 'node:crypto'
import { afterAll, beforeAll, expect, it, vi } from 'vitest'
import { inTransaction, type Db } from '../src/db.ts'
import type { EventMessage, OutboxEvent } from '../src/domain.ts'
import { createKafka, createPublisher, ensureTopics, runConsumer } from '../src/kafka.ts'
import { consumeWithRetry, enqueue, recordNotification, relayOnce } from '../src/outbox.ts'
import { testDb } from './helpers.ts'

let db: Db
beforeAll(async () => {
  db = await testDb()
})
afterAll(() => db.client.close())

const emit = (type: string, subscriptionId: string) =>
  inTransaction(db, (session) => enqueue(db, session, type, subscriptionId, { subscriptionId }))

it('does not store an event when its transaction rolls back', async () => {
  await expect(
    inTransaction(db, async (session) => {
      await enqueue(db, session, 'Lost', 'sub-rollback', { subscriptionId: 'sub-rollback' })
      throw new Error('business write failed')
    }),
  ).rejects.toThrow('business write failed')
  expect(await db.outbox.countDocuments({ key: 'sub-rollback' })).toBe(0)
})

it('publishes each pending event once', async () => {
  await emit('SubscriptionCreated', 'sub-1')
  await emit('PaymentSucceeded', 'sub-1')
  const published: OutboxEvent[] = []
  const publish = async (event: OutboxEvent) => {
    published.push(event)
  }
  expect(await relayOnce(db, publish)).toBe(2)
  expect(await relayOnce(db, publish)).toBe(0)
  expect(published.map((event) => event.type)).toEqual(['SubscriptionCreated', 'PaymentSucceeded'])
})

it('keeps a failed event and republishes it after the backoff', async () => {
  await emit('SubscriptionCancelled', 'sub-2')
  const now = new Date()
  const broken = vi.fn().mockRejectedValue(new Error('broker down'))
  expect(await relayOnce(db, broken, now)).toBe(0)
  expect(broken).toHaveBeenCalledTimes(1)
  expect(await relayOnce(db, broken, new Date(now.getTime() + 1_000))).toBe(0)
  expect(broken).toHaveBeenCalledTimes(1)

  const healthy = vi.fn().mockResolvedValue(undefined)
  expect(await relayOnce(db, healthy, new Date(now.getTime() + 2_000))).toBe(1)
  expect((await db.outbox.findOne({ key: 'sub-2' }))?.attempts).toBe(2)
})

it('applies a consumer effect once when the event is delivered twice', async () => {
  const event: EventMessage = {
    id: randomUUID(),
    type: 'PaymentFailed',
    key: 'sub-3',
    payload: { subscriptionId: 'sub-3' },
    createdAt: new Date().toISOString(),
  }
  expect(await recordNotification(db, event)).toBe(true)
  expect(await recordNotification(db, event)).toBe(false)
  expect(await db.notifications.countDocuments({ eventId: event.id })).toBe(1)
})

it('dead-letters a message after the handler fails three times', async () => {
  const handle = vi.fn().mockRejectedValue(new Error('bad payload'))
  const deadLetter = vi.fn().mockResolvedValue(undefined)
  await consumeWithRetry(handle, deadLetter)
  expect(handle).toHaveBeenCalledTimes(3)
  expect(deadLetter).toHaveBeenCalledWith('Error: bad payload')

  const flaky = vi.fn().mockRejectedValueOnce(new Error('transient')).mockResolvedValue(undefined)
  await consumeWithRetry(flaky, deadLetter)
  expect(flaky).toHaveBeenCalledTimes(2)
  expect(deadLetter).toHaveBeenCalledTimes(1)
})

it.skipIf(!process.env.KAFKA_BROKERS)('delivers an outbox event through Kafka to the consumer', async () => {
  const kafka = createKafka(process.env.KAFKA_BROKERS!.split(','))
  await ensureTopics(kafka)
  const publisher = await createPublisher(kafka)
  const stop = await runConsumer(kafka, `test-${randomUUID()}`, (event) => recordNotification(db, event))
  try {
    const subscriptionId = `sub-kafka-${randomUUID()}`
    await emit('SubscriptionCreated', subscriptionId)
    await relayOnce(db, publisher.publish)
    await vi.waitFor(
      async () => expect(await db.notifications.countDocuments({ subscriptionId })).toBe(1),
      { timeout: 45_000, interval: 500 },
    )
  } finally {
    await stop()
    await publisher.close()
  }
})
