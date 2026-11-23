import { randomUUID } from 'node:crypto'
import type { ClientSession } from 'mongodb'
import { inTransaction, type Db } from './db.ts'
import {
  addMonth,
  AppError,
  HOLD_MS,
  LIVE,
  statesLeadingTo,
  type Payment,
  type Subscription,
  type SubscriptionStatus,
} from './domain.ts'
import { enqueue } from './outbox.ts'

export function tryTransition(
  db: Db,
  id: string,
  to: SubscriptionStatus,
  session: ClientSession,
  from: readonly SubscriptionStatus[] = statesLeadingTo(to),
): Promise<Subscription | null> {
  const allowed = statesLeadingTo(to).filter((status) => from.includes(status))
  return db.subscriptions.findOneAndUpdate(
    { _id: id, status: { $in: allowed } },
    { $set: { status: to, live: LIVE.includes(to) } },
    { session, returnDocument: 'after' },
  )
}

export function newPayment(subscription: Subscription, periodStart: Date, now: Date): Payment {
  return {
    _id: randomUUID(),
    subscriptionId: subscription._id,
    customerId: subscription.customerId,
    amountCents: subscription.monthlyPriceCents,
    periodStart,
    periodEnd: addMonth(periodStart),
    status: 'pending',
    attempts: 0,
    nextAttemptAt: now,
    createdAt: now,
    updatedAt: now,
  }
}

export async function createSubscription(
  db: Db,
  input: { customerId: string; vehicleId: string; idempotencyKey: string },
  now = new Date(),
): Promise<Subscription> {
  const replay = async () => {
    const existing = await db.subscriptions.findOne({
      customerId: input.customerId,
      idempotencyKey: input.idempotencyKey,
    })
    if (existing && existing.vehicleId !== input.vehicleId) {
      throw new AppError(422, 'idempotency key was already used for a different vehicle')
    }
    return existing
  }

  const existing = await replay()
  if (existing) return existing
  try {
    return await inTransaction(db, async (session) => {
      const vehicle = await db.vehicles.findOneAndUpdate(
        { _id: input.vehicleId, status: 'available' },
        { $set: { status: 'reserved' } },
        { session },
      )
      if (!vehicle) throw new AppError(409, 'vehicle is not available')
      const subscription: Subscription = {
        _id: randomUUID(),
        customerId: input.customerId,
        vehicleId: vehicle._id,
        idempotencyKey: input.idempotencyKey,
        status: 'pending',
        live: true,
        monthlyPriceCents: vehicle.monthlyPriceCents,
        currentPeriodStart: now,
        currentPeriodEnd: addMonth(now),
        holdExpiresAt: new Date(now.getTime() + HOLD_MS),
        createdAt: now,
      }
      await db.subscriptions.insertOne(subscription, { session })
      await db.payments.insertOne(newPayment(subscription, now, now), { session })
      await enqueue(db, session, 'SubscriptionCreated', subscription._id, {
        subscriptionId: subscription._id,
        customerId: subscription.customerId,
        vehicleId: subscription.vehicleId,
      })
      return subscription
    })
  } catch (error) {
    const winner = await replay()
    if (winner) return winner
    throw error
  }
}

export async function endSubscription(
  db: Db,
  id: string,
  to: 'cancelled' | 'expired',
  session: ClientSession,
  from?: readonly SubscriptionStatus[],
): Promise<Subscription> {
  const subscription = await tryTransition(db, id, to, session, from)
  if (!subscription) throw new AppError(409, `subscription cannot become ${to}`)
  await db.vehicles.updateOne(
    { _id: subscription.vehicleId, status: { $in: ['reserved', 'subscribed'] } },
    { $set: { status: 'available' } },
    { session },
  )
  await db.payments.updateMany({ subscriptionId: id, status: 'pending' }, { $set: { status: 'void' } }, { session })
  await enqueue(db, session, to === 'cancelled' ? 'SubscriptionCancelled' : 'SubscriptionExpired', id, {
    subscriptionId: id,
    customerId: subscription.customerId,
    vehicleId: subscription.vehicleId,
  })
  return subscription
}

export function cancelSubscription(db: Db, id: string): Promise<Subscription> {
  return inTransaction(db, (session) => endSubscription(db, id, 'cancelled', session))
}

export async function expireHolds(db: Db, now = new Date()): Promise<number> {
  const stale = await db.subscriptions
    .find({ status: 'pending', holdExpiresAt: { $lte: now } }, { projection: { _id: 1 } })
    .toArray()
  let expired = 0
  for (const { _id } of stale) {
    try {
      await inTransaction(db, (session) => endSubscription(db, _id, 'expired', session, ['pending']))
      expired++
    } catch (error) {
      if (!(error instanceof AppError)) throw error
    }
  }
  return expired
}
