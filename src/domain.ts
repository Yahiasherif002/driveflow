export type Role = 'customer' | 'ops'
export type VehicleStatus = 'available' | 'reserved' | 'subscribed' | 'maintenance'
export type SubscriptionStatus = 'pending' | 'active' | 'past_due' | 'cancelled' | 'expired'
export type PaymentStatus = 'pending' | 'succeeded' | 'dead' | 'void'

export const TRANSITIONS: Record<SubscriptionStatus, readonly SubscriptionStatus[]> = {
  pending: ['active', 'cancelled', 'expired'],
  active: ['past_due', 'cancelled', 'expired'],
  past_due: ['active', 'cancelled', 'expired'],
  cancelled: [],
  expired: [],
}

export const LIVE: readonly SubscriptionStatus[] = ['pending', 'active', 'past_due']

export function statesLeadingTo(to: SubscriptionStatus): SubscriptionStatus[] {
  return (Object.keys(TRANSITIONS) as SubscriptionStatus[]).filter((from) => TRANSITIONS[from].includes(to))
}

export const HOLD_MS = 15 * 60_000
export const MAX_PAYMENT_ATTEMPTS = 4
export const RETRY_BASE_MS = 60 * 60_000
export const PROVIDER_ERROR_DELAY_MS = 60_000

export function backoffMs(attempts: number): number {
  return RETRY_BASE_MS * 2 ** (attempts - 1)
}

// ponytail: the billing day drifts down after a short month (Jan 31 -> Feb 28 -> Mar 28).
// Store an anchor day on the subscription if billing must return to the original day.
export function addMonth(date: Date): Date {
  const next = new Date(date)
  const day = next.getUTCDate()
  next.setUTCDate(1)
  next.setUTCMonth(next.getUTCMonth() + 1)
  const lastDay = new Date(Date.UTC(next.getUTCFullYear(), next.getUTCMonth() + 1, 0)).getUTCDate()
  next.setUTCDate(Math.min(day, lastDay))
  return next
}

export class AppError extends Error {
  statusCode: number

  constructor(statusCode: number, message: string) {
    super(message)
    this.statusCode = statusCode
  }
}

export interface User {
  _id: string
  email: string
  passwordHash: string
  role: Role
}

export interface Vehicle {
  _id: string
  plate: string
  model: string
  monthlyPriceCents: number
  status: VehicleStatus
}

export interface Subscription {
  _id: string
  customerId: string
  vehicleId: string
  idempotencyKey: string
  status: SubscriptionStatus
  live: boolean
  monthlyPriceCents: number
  currentPeriodStart: Date
  currentPeriodEnd: Date
  holdExpiresAt?: Date
  renewalPeriod?: Date
  createdAt: Date
}

export interface Payment {
  _id: string
  subscriptionId: string
  customerId: string
  amountCents: number
  periodStart: Date
  periodEnd: Date
  status: PaymentStatus
  attempts: number
  nextAttemptAt: Date
  providerChargeId?: string
  refundRequired?: boolean
  createdAt: Date
  updatedAt: Date
}

export interface OutboxEvent {
  _id: string
  topic: string
  type: string
  key: string
  payload: Record<string, unknown>
  createdAt: Date
  publishedAt: Date | null
  attempts: number
  lockedUntil: Date
}

export interface EventMessage {
  id: string
  type: string
  key: string
  payload: Record<string, unknown>
  createdAt: string
}

export interface Maintenance {
  _id: string
  vehicleId: string
  reason: string
  startedAt: Date
  completedAt: Date | null
}

export interface Notification {
  _id: string
  eventId: string
  type: string
  subscriptionId: string
  createdAt: Date
}
