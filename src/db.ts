import { MongoClient, type ClientSession, type Collection } from 'mongodb'
import type { Maintenance, Notification, OutboxEvent, Payment, Subscription, User, Vehicle } from './domain.ts'

export interface Db {
  client: MongoClient
  users: Collection<User>
  vehicles: Collection<Vehicle>
  subscriptions: Collection<Subscription>
  payments: Collection<Payment>
  outbox: Collection<OutboxEvent>
  maintenance: Collection<Maintenance>
  notifications: Collection<Notification>
  webhookEvents: Collection<{ _id: string; receivedAt: Date }>
  processedEvents: Collection<{ _id: string }>
}

const COLLECTIONS = [
  'users',
  'vehicles',
  'subscriptions',
  'payments',
  'outbox',
  'maintenance',
  'notifications',
  'webhook_events',
  'processed_events',
]
const NAMESPACE_EXISTS = 48

export async function connect(url: string, dbName = 'driveflow'): Promise<Db> {
  const client = await MongoClient.connect(url)
  const database = client.db(dbName)
  await Promise.all(
    COLLECTIONS.map((name) =>
      database.createCollection(name).catch((error: { code?: number }) => {
        if (error.code !== NAMESPACE_EXISTS) throw error
      }),
    ),
  )
  const db: Db = {
    client,
    users: database.collection('users'),
    vehicles: database.collection('vehicles'),
    subscriptions: database.collection('subscriptions'),
    payments: database.collection('payments'),
    outbox: database.collection('outbox'),
    maintenance: database.collection('maintenance'),
    notifications: database.collection('notifications'),
    webhookEvents: database.collection('webhook_events'),
    processedEvents: database.collection('processed_events'),
  }
  await Promise.all([
    db.users.createIndex({ email: 1 }, { unique: true }),
    db.vehicles.createIndex({ plate: 1 }, { unique: true }),
    db.vehicles.createIndex({ status: 1, _id: 1 }),
    db.subscriptions.createIndex({ vehicleId: 1 }, { unique: true, partialFilterExpression: { live: true } }),
    db.subscriptions.createIndex({ customerId: 1, idempotencyKey: 1 }, { unique: true }),
    db.subscriptions.createIndex({ status: 1, currentPeriodEnd: 1 }),
    db.subscriptions.createIndex({ status: 1, holdExpiresAt: 1 }),
    db.payments.createIndex({ subscriptionId: 1, periodStart: 1 }, { unique: true }),
    db.payments.createIndex({ status: 1, nextAttemptAt: 1 }),
    db.outbox.createIndex({ publishedAt: 1, lockedUntil: 1, createdAt: 1 }),
    db.maintenance.createIndex({ vehicleId: 1, completedAt: 1 }),
  ])
  return db
}

export function isDuplicateKey(error: unknown): boolean {
  return (error as { code?: number } | null)?.code === 11000
}

export function inTransaction<T>(db: Db, work: (session: ClientSession) => Promise<T>): Promise<T> {
  return db.client.withSession((session) => session.withTransaction(() => work(session)))
}
