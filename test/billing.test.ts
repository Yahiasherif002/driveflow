import { createHmac } from 'node:crypto'
import { afterEach, beforeEach, expect, it } from 'vitest'
import { applyCharge, chargeKey, FakeProvider, reconcile, runBilling, verifyWebhook } from '../src/billing.ts'
import type { Db } from '../src/db.ts'
import { backoffMs, MAX_PAYMENT_ATTEMPTS, PROVIDER_ERROR_DELAY_MS } from '../src/domain.ts'
import { cancelSubscription, createSubscription } from '../src/subscriptions.ts'
import { addVehicle, testDb } from './helpers.ts'

let db: Db
beforeEach(async () => {
  db = await testDb()
})
afterEach(() => db.client.close())

let counter = 0
async function subscribe(now: Date) {
  const vehicle = await addVehicle(db)
  const subscription = await createSubscription(
    db,
    { customerId: `customer-${++counter}`, vehicleId: vehicle._id, idempotencyKey: `billing-key-${counter}` },
    now,
  )
  return { vehicle, subscription }
}

const status = async (id: string) => (await db.subscriptions.findOne({ _id: id }))?.status
const paymentsOf = (id: string) => db.payments.find({ subscriptionId: id }).sort({ periodStart: 1 }).toArray()

it('activates the subscription when the first charge succeeds', async () => {
  const now = new Date('2026-03-01T00:00:00Z')
  const { vehicle, subscription } = await subscribe(now)
  await runBilling(db, new FakeProvider(), now)
  expect(await status(subscription._id)).toBe('active')
  expect((await db.vehicles.findOne({ _id: vehicle._id }))?.status).toBe('subscribed')
  expect(await db.outbox.countDocuments({ type: 'PaymentSucceeded', key: subscription._id })).toBe(1)
})

it('does not charge twice when the provider response is lost', async () => {
  const now = new Date('2026-03-01T00:00:00Z')
  const { subscription } = await subscribe(now)
  const provider = new FakeProvider()
  provider.dropResponses = 1
  await runBilling(db, provider, now)
  expect(await status(subscription._id)).toBe('pending')

  await runBilling(db, provider, new Date(now.getTime() + PROVIDER_ERROR_DELAY_MS))
  expect(provider.charges.size).toBe(1)
  expect(await status(subscription._id)).toBe('active')
})

it('reconciliation recovers a charge the service never recorded', async () => {
  const now = new Date('2026-03-01T00:00:00Z')
  const { subscription } = await subscribe(now)
  const provider = new FakeProvider()
  provider.dropResponses = 1
  await runBilling(db, provider, now)

  expect(await reconcile(db, provider, now)).toBe(1)
  expect(await reconcile(db, provider, now)).toBe(0)
  expect(await status(subscription._id)).toBe('active')
  expect((await paymentsOf(subscription._id))[0].status).toBe('succeeded')
})

it('bills each renewal period once and advances the period', async () => {
  const start = new Date('2026-03-01T00:00:00Z')
  const { subscription } = await subscribe(start)
  const provider = new FakeProvider()
  await runBilling(db, provider, start)

  const renewal = new Date('2026-04-01T00:00:00Z')
  provider.dropResponses = 1
  await runBilling(db, provider, renewal)
  await runBilling(db, provider, renewal)
  await runBilling(db, provider, new Date(renewal.getTime() + PROVIDER_ERROR_DELAY_MS))

  const payments = await paymentsOf(subscription._id)
  expect(payments.map((payment) => payment.status)).toEqual(['succeeded', 'succeeded'])
  expect(provider.charges.size).toBe(2)
  const renewed = await db.subscriptions.findOne({ _id: subscription._id })
  expect(renewed?.currentPeriodEnd.toISOString()).toBe('2026-05-01T00:00:00.000Z')
})

it('retries a declined renewal with backoff, then recovers', async () => {
  const start = new Date('2026-03-01T00:00:00Z')
  const { subscription } = await subscribe(start)
  const provider = new FakeProvider()
  await runBilling(db, provider, start)

  const renewal = new Date('2026-04-01T00:00:00Z')
  provider.decline = () => true
  await runBilling(db, provider, renewal)
  expect(await status(subscription._id)).toBe('past_due')

  provider.decline = () => false
  await runBilling(db, provider, new Date(renewal.getTime() + backoffMs(1) - 1))
  expect(await status(subscription._id)).toBe('past_due')
  await runBilling(db, provider, new Date(renewal.getTime() + backoffMs(1)))
  expect(await status(subscription._id)).toBe('active')
})

it('dead-letters the payment and expires the subscription after the last attempt', async () => {
  const start = new Date('2026-03-01T00:00:00Z')
  const { vehicle, subscription } = await subscribe(start)
  const provider = new FakeProvider()
  await runBilling(db, provider, start)

  provider.decline = () => true
  let now = new Date('2026-04-01T00:00:00Z')
  for (let attempt = 1; attempt <= MAX_PAYMENT_ATTEMPTS; attempt++) {
    await runBilling(db, provider, now)
    now = new Date(now.getTime() + backoffMs(attempt))
  }
  const [, renewalPayment] = await paymentsOf(subscription._id)
  expect(renewalPayment).toMatchObject({ status: 'dead', attempts: MAX_PAYMENT_ATTEMPTS })
  expect(await status(subscription._id)).toBe('expired')
  expect((await db.vehicles.findOne({ _id: vehicle._id }))?.status).toBe('available')
  expect(await db.outbox.countDocuments({ type: 'PaymentDeadLettered', key: subscription._id })).toBe(1)
})

it('applies a webhook event once and ignores redelivery', async () => {
  const now = new Date('2026-03-01T00:00:00Z')
  const { subscription } = await subscribe(now)
  const [payment] = await paymentsOf(subscription._id)
  const failed = { chargeId: 'ch_1', idempotencyKey: chargeKey(payment._id, 0), status: 'failed' as const }

  expect(await applyCharge(db, failed, { eventId: 'evt_1', now })).toBe('applied')
  expect(await applyCharge(db, failed, { eventId: 'evt_1', now })).toBe('duplicate')
  expect(await applyCharge(db, failed, { eventId: 'evt_2', now })).toBe('duplicate')
  expect((await paymentsOf(subscription._id))[0].attempts).toBe(1)
})

it('flags a refund when money arrives for a cancelled subscription', async () => {
  const now = new Date('2026-03-01T00:00:00Z')
  const { subscription } = await subscribe(now)
  const [payment] = await paymentsOf(subscription._id)
  await cancelSubscription(db, subscription._id)

  const late = { chargeId: 'ch_late', idempotencyKey: chargeKey(payment._id, 0), status: 'succeeded' as const }
  expect(await applyCharge(db, late, { now })).toBe('refund_required')
  expect(await applyCharge(db, late, { now })).toBe('duplicate')
  expect(await status(subscription._id)).toBe('cancelled')
  expect(await db.outbox.countDocuments({ type: 'RefundRequired', key: subscription._id })).toBe(1)
})

it('accepts only a correctly signed webhook body', () => {
  const body = '{"id":"evt_1"}'
  const signature = createHmac('sha256', 'secret').update(body).digest('hex')
  expect(verifyWebhook(body, signature, 'secret')).toBe(true)
  expect(verifyWebhook(body + ' ', signature, 'secret')).toBe(false)
  expect(verifyWebhook(body, signature, 'other-secret')).toBe(false)
  expect(verifyWebhook(body, undefined, 'secret')).toBe(false)
})
