import { randomUUID } from 'node:crypto'
import type { ClientSession } from 'mongodb'
import { inTransaction, isDuplicateKey, type Db } from './db.ts'
import type { EventMessage, OutboxEvent } from './domain.ts'

export const TOPIC = 'driveflow.events'
export const DLQ_TOPIC = 'driveflow.events.dlq'
const LEASE_MS = 30_000
const MAX_PUBLISH_BACKOFF_MS = 300_000

export type Publish = (event: OutboxEvent) => Promise<void>

export async function enqueue(
  db: Db,
  session: ClientSession,
  type: string,
  key: string,
  payload: Record<string, unknown>,
): Promise<void> {
  await db.outbox.insertOne(
    {
      _id: randomUUID(),
      topic: TOPIC,
      type,
      key,
      payload,
      createdAt: new Date(),
      publishedAt: null,
      attempts: 0,
      lockedUntil: new Date(0),
    },
    { session },
  )
}

// ponytail: delivery is at-least-once and a failed event does not block later events,
// so per-key ordering is best effort. Relay one partition key at a time if strict order is needed.
export async function relayOnce(db: Db, publish: Publish, now = new Date(), limit = 100): Promise<number> {
  let published = 0
  for (let i = 0; i < limit; i++) {
    const event = await db.outbox.findOneAndUpdate(
      { publishedAt: null, lockedUntil: { $lte: now } },
      { $set: { lockedUntil: new Date(now.getTime() + LEASE_MS) }, $inc: { attempts: 1 } },
      { sort: { createdAt: 1 }, returnDocument: 'after' },
    )
    if (!event) break
    try {
      await publish(event)
      await db.outbox.updateOne({ _id: event._id }, { $set: { publishedAt: new Date() } })
      published++
    } catch {
      const delay = Math.min(1000 * 2 ** event.attempts, MAX_PUBLISH_BACKOFF_MS)
      await db.outbox.updateOne({ _id: event._id }, { $set: { lockedUntil: new Date(now.getTime() + delay) } })
    }
  }
  return published
}

export async function handleOnce(
  db: Db,
  consumer: string,
  eventId: string,
  effect: (session: ClientSession) => Promise<void>,
): Promise<boolean> {
  try {
    await inTransaction(db, async (session) => {
      await db.processedEvents.insertOne({ _id: `${consumer}:${eventId}` }, { session })
      await effect(session)
    })
    return true
  } catch (error) {
    if (isDuplicateKey(error)) return false
    throw error
  }
}

// ponytail: retries are immediate. Add a delay or a retry topic once handlers call slow external services.
export async function consumeWithRetry(
  handle: () => Promise<unknown>,
  deadLetter: (error: string) => Promise<void>,
  maxAttempts = 3,
): Promise<void> {
  for (let attempt = 1; ; attempt++) {
    try {
      await handle()
      return
    } catch (error) {
      if (attempt >= maxAttempts) {
        await deadLetter(String(error))
        return
      }
    }
  }
}

export function recordNotification(db: Db, event: EventMessage): Promise<boolean> {
  return handleOnce(db, 'notifications', event.id, async (session) => {
    await db.notifications.insertOne(
      {
        _id: randomUUID(),
        eventId: event.id,
        type: event.type,
        subscriptionId: String(event.payload.subscriptionId),
        createdAt: new Date(),
      },
      { session },
    )
  })
}
