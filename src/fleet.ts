import { randomUUID } from 'node:crypto'
import { inTransaction, type Db } from './db.ts'
import { AppError, type Maintenance, type SubscriptionStatus, type VehicleStatus } from './domain.ts'

export function startMaintenance(db: Db, vehicleId: string, reason: string, now = new Date()): Promise<Maintenance> {
  return inTransaction(db, async (session) => {
    const vehicle = await db.vehicles.findOneAndUpdate(
      { _id: vehicleId, status: 'available' },
      { $set: { status: 'maintenance' } },
      { session },
    )
    if (!vehicle) throw new AppError(409, 'only an available vehicle can enter maintenance')
    const record: Maintenance = { _id: randomUUID(), vehicleId, reason, startedAt: now, completedAt: null }
    await db.maintenance.insertOne(record, { session })
    return record
  })
}

export function completeMaintenance(db: Db, id: string, now = new Date()): Promise<Maintenance> {
  return inTransaction(db, async (session) => {
    const record = await db.maintenance.findOneAndUpdate(
      { _id: id, completedAt: null },
      { $set: { completedAt: now } },
      { session, returnDocument: 'after' },
    )
    if (!record) throw new AppError(409, 'maintenance record is not open')
    await db.vehicles.updateOne(
      { _id: record.vehicleId, status: 'maintenance' },
      { $set: { status: 'available' } },
      { session },
    )
    return record
  })
}

export async function dashboard(db: Db, now = new Date()) {
  const inSevenDays = new Date(now.getTime() + 7 * 86_400_000)
  const [vehicleCounts, subscriptionCounts, overdue, renewalsDueIn7Days] = await Promise.all([
    db.vehicles.aggregate<{ _id: VehicleStatus; count: number }>([{ $group: { _id: '$status', count: { $sum: 1 } } }]).toArray(),
    db.subscriptions
      .aggregate<{ _id: SubscriptionStatus; count: number }>([
        { $match: { live: true } },
        { $group: { _id: '$status', count: { $sum: 1 } } },
      ])
      .toArray(),
    db.payments
      .aggregate<{ count: number; amountCents: number }>([
        { $match: { status: 'pending', attempts: { $gt: 0 } } },
        { $group: { _id: null, count: { $sum: 1 }, amountCents: { $sum: '$amountCents' } } },
      ])
      .toArray(),
    db.subscriptions.countDocuments({ status: 'active', currentPeriodEnd: { $gt: now, $lte: inSevenDays } }),
  ])
  const vehicles = { available: 0, reserved: 0, subscribed: 0, maintenance: 0 }
  for (const { _id, count } of vehicleCounts) vehicles[_id] = count
  const subscriptions = { pending: 0, active: 0, past_due: 0 }
  for (const { _id, count } of subscriptionCounts) {
    if (_id in subscriptions) subscriptions[_id as keyof typeof subscriptions] = count
  }
  const fleetSize = Object.values(vehicles).reduce((sum, count) => sum + count, 0)
  return {
    vehicles,
    fleetSize,
    utilization: fleetSize ? vehicles.subscribed / fleetSize : 0,
    subscriptions,
    overduePayments: { count: overdue[0]?.count ?? 0, amountCents: overdue[0]?.amountCents ?? 0 },
    renewalsDueIn7Days,
  }
}
