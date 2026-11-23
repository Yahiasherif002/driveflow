import { randomUUID } from 'node:crypto'
import { connect, type Db } from '../src/db.ts'
import type { Vehicle } from '../src/domain.ts'

export function testDb(): Promise<Db> {
  return connect(process.env.MONGO_TEST_URL!, `t_${randomUUID()}`)
}

export async function addVehicle(db: Db): Promise<Vehicle> {
  const vehicle: Vehicle = {
    _id: randomUUID(),
    plate: randomUUID().slice(0, 8),
    model: 'Toyota Corolla',
    monthlyPriceCents: 150_000,
    status: 'available',
  }
  await db.vehicles.insertOne(vehicle)
  return vehicle
}
