import { createHmac, randomUUID, timingSafeEqual } from 'node:crypto'
import type { ClientSession } from 'mongodb'
import { inTransaction, isDuplicateKey, type Db } from './db.ts'
import { backoffMs, MAX_PAYMENT_ATTEMPTS, PROVIDER_ERROR_DELAY_MS } from './domain.ts'
import { enqueue } from './outbox.ts'
import { endSubscription, newPayment, tryTransition } from './subscriptions.ts'

export interface Charge {
  chargeId: string
  idempotencyKey: string
  status: 'succeeded' | 'failed'
}

export interface PaymentProvider {
  charge(request: { idempotencyKey: string; amountCents: number; customerId: string }): Promise<Charge>
  listCharges(): Promise<Charge[]>
}

export class FakeProvider implements PaymentProvider {
  charges = new Map<string, Charge>()
  decline: (idempotencyKey: string) => boolean = () => false
  dropResponses = 0

  async charge(request: { idempotencyKey: string }): Promise<Charge> {
    let charge = this.charges.get(request.idempotencyKey)
    if (!charge) {
      charge = {
        chargeId: randomUUID(),
        idempotencyKey: request.idempotencyKey,
        status: this.decline(request.idempotencyKey) ? 'failed' : 'succeeded',
      }
      this.charges.set(request.idempotencyKey, charge)
    }
    if (this.dropResponses > 0) {
      this.dropResponses--
      throw new Error('provider timeout')
    }
    return charge
  }

  async listCharges(): Promise<Charge[]> {
    return [...this.charges.values()]
  }
}

export type ApplyOutcome = 'applied' | 'duplicate' | 'refund_required'

export function chargeKey(paymentId: string, attempts: number): string {
  return `${paymentId}:${attempts}`
}

function parseChargeKey(key: string): { paymentId: string; attempt: number } {
  const separator = key.lastIndexOf(':')
  return { paymentId: key.slice(0, separator), attempt: Number(key.slice(separator + 1)) }
}

export function verifyWebhook(rawBody: string, signature: unknown, secret: string): boolean {
  if (typeof signature !== 'string') return false
  const expected = createHmac('sha256', secret).update(rawBody).digest()
  const given = Buffer.from(signature, 'hex')
  return given.length === expected.length && timingSafeEqual(given, expected)
}

export async function applyCharge(
  db: Db,
  charge: Charge,
  options: { eventId?: string; now?: Date } = {},
): Promise<ApplyOutcome> {
  const now = options.now ?? new Date()
  try {
    return await inTransaction(db, async (session) => {
      if (options.eventId) {
        await db.webhookEvents.insertOne({ _id: options.eventId, receivedAt: now }, { session })
      }
      return charge.status === 'succeeded'
        ? applySuccess(db, charge, now, session)
        : applyFailure(db, charge, now, session)
    })
  } catch (error) {
    if (isDuplicateKey(error)) return 'duplicate'
    throw error
  }
}

async function applySuccess(db: Db, charge: Charge, now: Date, session: ClientSession): Promise<ApplyOutcome> {
  const { paymentId } = parseChargeKey(charge.idempotencyKey)
  const payment = await db.payments.findOneAndUpdate(
    { _id: paymentId, status: 'pending' },
    { $set: { status: 'succeeded', providerChargeId: charge.chargeId, updatedAt: now } },
    { session },
  )
  if (!payment) {
    const orphan = await db.payments.findOneAndUpdate(
      { _id: paymentId, status: { $in: ['dead', 'void'] }, refundRequired: { $ne: true } },
      { $set: { refundRequired: true, providerChargeId: charge.chargeId, updatedAt: now } },
      { session },
    )
    if (!orphan) return 'duplicate'
    await enqueue(db, session, 'RefundRequired', orphan.subscriptionId, {
      subscriptionId: orphan.subscriptionId,
      paymentId,
      amountCents: orphan.amountCents,
    })
    return 'refund_required'
  }
  await tryTransition(db, payment.subscriptionId, 'active', session)
  const subscription = await db.subscriptions.findOneAndUpdate(
    { _id: payment.subscriptionId, status: 'active' },
    {
      $set: { currentPeriodStart: payment.periodStart, currentPeriodEnd: payment.periodEnd },
      $unset: { holdExpiresAt: '' },
    },
    { session },
  )
  if (!subscription) throw new Error(`pending payment ${paymentId} belongs to an ended subscription`)
  await db.vehicles.updateOne(
    { _id: subscription.vehicleId, status: 'reserved' },
    { $set: { status: 'subscribed' } },
    { session },
  )
  await enqueue(db, session, 'PaymentSucceeded', payment.subscriptionId, {
    subscriptionId: payment.subscriptionId,
    paymentId,
    amountCents: payment.amountCents,
  })
  return 'applied'
}

async function applyFailure(db: Db, charge: Charge, now: Date, session: ClientSession): Promise<ApplyOutcome> {
  const { paymentId, attempt } = parseChargeKey(charge.idempotencyKey)
  const payment = await db.payments.findOneAndUpdate(
    { _id: paymentId, status: 'pending', attempts: attempt },
    { $inc: { attempts: 1 }, $set: { updatedAt: now } },
    { session, returnDocument: 'after' },
  )
  if (!payment) return 'duplicate'
  const payload = { subscriptionId: payment.subscriptionId, paymentId, attempts: payment.attempts }
  if (payment.attempts >= MAX_PAYMENT_ATTEMPTS) {
    await db.payments.updateOne({ _id: paymentId }, { $set: { status: 'dead' } }, { session })
    await endSubscription(db, payment.subscriptionId, 'expired', session)
    await enqueue(db, session, 'PaymentDeadLettered', payment.subscriptionId, payload)
  } else {
    await db.payments.updateOne(
      { _id: paymentId },
      { $set: { nextAttemptAt: new Date(now.getTime() + backoffMs(payment.attempts)) } },
      { session },
    )
    await tryTransition(db, payment.subscriptionId, 'past_due', session)
    await enqueue(db, session, 'PaymentFailed', payment.subscriptionId, payload)
  }
  return 'applied'
}

async function openRenewalPayments(db: Db, now: Date): Promise<void> {
  const due = await db.subscriptions
    .find({
      status: 'active',
      currentPeriodEnd: { $lte: now },
      $expr: { $ne: ['$renewalPeriod', '$currentPeriodEnd'] },
    })
    .toArray()
  for (const subscription of due) {
    await inTransaction(db, async (session) => {
      const claimed = await db.subscriptions.findOneAndUpdate(
        {
          _id: subscription._id,
          status: 'active',
          currentPeriodEnd: subscription.currentPeriodEnd,
          renewalPeriod: { $ne: subscription.currentPeriodEnd },
        },
        { $set: { renewalPeriod: subscription.currentPeriodEnd } },
        { session },
      )
      if (claimed) {
        await db.payments.insertOne(newPayment(claimed, claimed.currentPeriodEnd, now), { session })
      }
    })
  }
}

export async function runBilling(db: Db, provider: PaymentProvider, now = new Date()): Promise<number> {
  await openRenewalPayments(db, now)
  const due = await db.payments.find({ status: 'pending', nextAttemptAt: { $lte: now } }).toArray()
  for (const payment of due) {
    let charge: Charge
    try {
      charge = await provider.charge({
        idempotencyKey: chargeKey(payment._id, payment.attempts),
        amountCents: payment.amountCents,
        customerId: payment.customerId,
      })
    } catch {
      await db.payments.updateOne(
        { _id: payment._id, status: 'pending' },
        { $set: { nextAttemptAt: new Date(now.getTime() + PROVIDER_ERROR_DELAY_MS) } },
      )
      continue
    }
    await applyCharge(db, charge, { now })
  }
  return due.length
}

export async function reconcile(db: Db, provider: PaymentProvider, now = new Date()): Promise<number> {
  let repaired = 0
  for (const charge of await provider.listCharges()) {
    if ((await applyCharge(db, charge, { now })) !== 'duplicate') repaired++
  }
  return repaired
}
