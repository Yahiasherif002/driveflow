import { createHmac } from 'node:crypto'
import { afterAll, beforeAll, expect, it } from 'vitest'
import { buildApp } from '../src/app.ts'
import { chargeKey } from '../src/billing.ts'
import type { Db } from '../src/db.ts'
import { addVehicle, testDb } from './helpers.ts'

const WEBHOOK_SECRET = 'test-webhook-secret'
let db: Db
let app: Awaited<ReturnType<typeof buildApp>>
let opsToken: string

beforeAll(async () => {
  db = await testDb()
  app = await buildApp({ db, jwtSecret: 'test-jwt-secret', webhookSecret: WEBHOOK_SECRET })
  opsToken = app.jwt.sign({ sub: 'ops-user', role: 'ops' })
})
afterAll(async () => {
  await app.close()
  await db.client.close()
})

const bearer = (token: string) => ({ authorization: `Bearer ${token}` })

async function register(email: string): Promise<string> {
  const response = await app.inject({
    method: 'POST',
    url: '/auth/register',
    payload: { email, password: 'correct horse battery' },
  })
  expect(response.statusCode).toBe(201)
  return response.json().token
}

it('registers, logs in, and rejects bad credentials', async () => {
  await register('login@example.com')
  const login = (password: string) =>
    app.inject({ method: 'POST', url: '/auth/login', payload: { email: 'Login@Example.com', password } })
  expect((await login('correct horse battery')).statusCode).toBe(200)
  expect((await login('wrong horse battery')).statusCode).toBe(401)
  const duplicate = await app.inject({
    method: 'POST',
    url: '/auth/register',
    payload: { email: 'login@example.com', password: 'correct horse battery' },
  })
  expect(duplicate.statusCode).toBe(409)
  const weak = await app.inject({
    method: 'POST',
    url: '/auth/register',
    payload: { email: 'weak@example.com', password: 'short' },
  })
  expect(weak.statusCode).toBe(400)
})

it('enforces authentication and roles', async () => {
  const customer = await register('roles@example.com')
  expect((await app.inject({ url: '/vehicles' })).statusCode).toBe(401)
  expect((await app.inject({ url: '/ops/dashboard', headers: bearer(customer) })).statusCode).toBe(403)
  const created = await app.inject({
    method: 'POST',
    url: '/vehicles',
    headers: bearer(customer),
    payload: { plate: 'X-1', model: 'Kia', monthlyPriceCents: 1000 },
  })
  expect(created.statusCode).toBe(403)
})

it('books a vehicle idempotently and rejects a second customer', async () => {
  const [first, second] = await Promise.all([register('first@example.com'), register('second@example.com')])
  const vehicle = await addVehicle(db)
  const book = (token: string, key?: string) =>
    app.inject({
      method: 'POST',
      url: '/subscriptions',
      headers: { ...bearer(token), ...(key && { 'idempotency-key': key }) },
      payload: { vehicleId: vehicle._id },
    })

  expect((await book(first)).statusCode).toBe(400)
  const created = await book(first, 'booking-key-1')
  expect(created.statusCode).toBe(201)
  expect((await book(first, 'booking-key-1')).json()._id).toBe(created.json()._id)
  expect((await book(second, 'booking-key-2')).statusCode).toBe(409)

  const url = `/subscriptions/${created.json()._id}`
  expect((await app.inject({ url, headers: bearer(second) })).statusCode).toBe(404)
  expect((await app.inject({ url, headers: bearer(first) })).json().status).toBe('pending')
})

it('activates a subscription from a signed webhook and rejects a forged one', async () => {
  const token = await register('webhook@example.com')
  const vehicle = await addVehicle(db)
  const created = await app.inject({
    method: 'POST',
    url: '/subscriptions',
    headers: { ...bearer(token), 'idempotency-key': 'webhook-key-1' },
    payload: { vehicleId: vehicle._id },
  })
  const payment = await db.payments.findOne({ subscriptionId: created.json()._id })
  const body = JSON.stringify({
    id: 'evt_http_1',
    type: 'charge.succeeded',
    data: { chargeId: 'ch_http_1', idempotencyKey: chargeKey(payment!._id, 0) },
  })
  const deliver = (signature: string) =>
    app.inject({
      method: 'POST',
      url: '/webhooks/payments',
      headers: { 'content-type': 'application/json', 'x-signature': signature },
      payload: body,
    })
  const signature = createHmac('sha256', WEBHOOK_SECRET).update(body).digest('hex')

  expect((await deliver('00'.repeat(32))).statusCode).toBe(401)
  expect((await deliver(signature)).json()).toEqual({ outcome: 'applied' })
  expect((await deliver(signature)).json()).toEqual({ outcome: 'duplicate' })
  const subscription = await app.inject({ url: `/subscriptions/${created.json()._id}`, headers: bearer(token) })
  expect(subscription.json().status).toBe('active')
})

it('reports fleet and subscription metrics to operations', async () => {
  const response = await app.inject({ url: '/ops/dashboard', headers: bearer(opsToken) })
  expect(response.statusCode).toBe(200)
  expect(response.json()).toMatchObject({
    fleetSize: 2,
    vehicles: { available: 0, reserved: 1, subscribed: 1, maintenance: 0 },
    utilization: 0.5,
    subscriptions: { pending: 1, active: 1, past_due: 0 },
    overduePayments: { count: 0, amountCents: 0 },
  })
})

it('paginates the vehicle list and reports readiness', async () => {
  const page = await app.inject({ url: '/vehicles?limit=1', headers: bearer(opsToken) })
  expect(page.json().items).toHaveLength(1)
  const next = await app.inject({ url: `/vehicles?limit=1&after=${page.json().next}`, headers: bearer(opsToken) })
  expect(next.json().items[0]._id).not.toBe(page.json().items[0]._id)
  expect((await app.inject({ url: '/readyz' })).json()).toEqual({ ok: true })
})
