import { afterEach, beforeEach, expect, it } from 'vitest'
import type { Db } from '../src/db.ts'
import { addMonth, AppError, HOLD_MS, statesLeadingTo, TRANSITIONS } from '../src/domain.ts'
import { completeMaintenance, startMaintenance } from '../src/fleet.ts'
import { cancelSubscription, createSubscription, expireHolds } from '../src/subscriptions.ts'
import { addVehicle, testDb } from './helpers.ts'

let db: Db
beforeEach(async () => {
  db = await testDb()
})
afterEach(() => db.client.close())

it('declares terminal states and legal transitions', () => {
  expect(TRANSITIONS.cancelled).toEqual([])
  expect(TRANSITIONS.expired).toEqual([])
  expect(statesLeadingTo('active')).toEqual(['pending', 'past_due'])
  expect(statesLeadingTo('past_due')).toEqual(['active'])
})

it('clamps month arithmetic to the last day of a short month', () => {
  expect(addMonth(new Date('2026-01-31T10:00:00Z')).toISOString()).toBe('2026-02-28T10:00:00.000Z')
  expect(addMonth(new Date('2026-12-15T00:00:00Z')).toISOString()).toBe('2027-01-15T00:00:00.000Z')
})

it('lets exactly one of 200 concurrent requests reserve a vehicle', async () => {
  const vehicle = await addVehicle(db)
  const results = await Promise.allSettled(
    Array.from({ length: 200 }, (_, i) =>
      createSubscription(db, { customerId: `customer-${i}`, vehicleId: vehicle._id, idempotencyKey: `key-${i}` }),
    ),
  )
  const rejected = results.filter((result) => result.status === 'rejected')
  expect(results.filter((result) => result.status === 'fulfilled')).toHaveLength(1)
  expect(rejected).toHaveLength(199)
  for (const result of rejected) expect(result.reason).toMatchObject({ statusCode: 409 })
  expect(await db.subscriptions.countDocuments({ vehicleId: vehicle._id })).toBe(1)
  expect(await db.payments.countDocuments({})).toBe(1)
  expect(await db.outbox.countDocuments({ type: 'SubscriptionCreated' })).toBe(1)
  expect((await db.vehicles.findOne({ _id: vehicle._id }))?.status).toBe('reserved')
})

it('returns the same subscription when a request is retried with the same idempotency key', async () => {
  const vehicle = await addVehicle(db)
  const input = { customerId: 'retrying-customer', vehicleId: vehicle._id, idempotencyKey: 'retry-key-1' }
  const results = await Promise.all(Array.from({ length: 20 }, () => createSubscription(db, input)))
  expect(new Set(results.map((subscription) => subscription._id)).size).toBe(1)
  expect(await db.subscriptions.countDocuments({ customerId: 'retrying-customer' })).toBe(1)

  const other = await addVehicle(db)
  await expect(createSubscription(db, { ...input, vehicleId: other._id })).rejects.toMatchObject({ statusCode: 422 })
})

it('releases the vehicle when a payment hold expires', async () => {
  const vehicle = await addVehicle(db)
  const now = new Date()
  const subscription = await createSubscription(
    db,
    { customerId: 'slow-payer', vehicleId: vehicle._id, idempotencyKey: 'hold-key-1' },
    now,
  )
  expect(await expireHolds(db, new Date(now.getTime() + HOLD_MS - 1))).toBe(0)
  expect(await expireHolds(db, new Date(now.getTime() + HOLD_MS))).toBe(1)
  expect((await db.subscriptions.findOne({ _id: subscription._id }))?.status).toBe('expired')
  expect((await db.vehicles.findOne({ _id: vehicle._id }))?.status).toBe('available')
  expect((await db.payments.findOne({ subscriptionId: subscription._id }))?.status).toBe('void')
})

it('frees the vehicle on cancellation and rejects a second cancellation', async () => {
  const vehicle = await addVehicle(db)
  const subscription = await createSubscription(db, {
    customerId: 'canceller',
    vehicleId: vehicle._id,
    idempotencyKey: 'cancel-key-1',
  })
  await cancelSubscription(db, subscription._id)
  expect((await db.vehicles.findOne({ _id: vehicle._id }))?.status).toBe('available')
  await expect(cancelSubscription(db, subscription._id)).rejects.toBeInstanceOf(AppError)
  await createSubscription(db, { customerId: 'next', vehicleId: vehicle._id, idempotencyKey: 'cancel-key-2' })
})

it('blocks booking while a vehicle is in maintenance', async () => {
  const vehicle = await addVehicle(db)
  const record = await startMaintenance(db, vehicle._id, 'brake service')
  const input = { customerId: 'waiting', vehicleId: vehicle._id, idempotencyKey: 'maintenance-key-1' }
  await expect(createSubscription(db, input)).rejects.toMatchObject({ statusCode: 409 })
  await expect(startMaintenance(db, vehicle._id, 'again')).rejects.toMatchObject({ statusCode: 409 })
  await completeMaintenance(db, record._id)
  expect((await createSubscription(db, input)).status).toBe('pending')
})
