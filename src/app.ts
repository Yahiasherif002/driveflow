import { randomBytes, randomUUID, scrypt, timingSafeEqual } from 'node:crypto'
import { promisify } from 'node:util'
import fastifyJwt from '@fastify/jwt'
import rateLimit from '@fastify/rate-limit'
import Fastify, { type FastifyRequest } from 'fastify'
import { applyCharge, verifyWebhook } from './billing.ts'
import { isDuplicateKey, type Db } from './db.ts'
import { AppError, type Role, type VehicleStatus } from './domain.ts'
import { completeMaintenance, dashboard, startMaintenance } from './fleet.ts'
import { cancelSubscription, createSubscription } from './subscriptions.ts'

declare module 'fastify' {
  interface FastifyRequest {
    rawBody?: string
  }
}

declare module '@fastify/jwt' {
  interface FastifyJWT {
    payload: { sub: string; role: Role }
    user: { sub: string; role: Role }
  }
}

const scryptAsync = promisify(scrypt) as (password: string, salt: Buffer, keyLength: number) => Promise<Buffer>

export async function hashPassword(password: string): Promise<string> {
  const salt = randomBytes(16)
  const hash = await scryptAsync(password, salt, 64)
  return `${salt.toString('hex')}:${hash.toString('hex')}`
}

async function verifyPassword(password: string, stored: string): Promise<boolean> {
  const [salt, hash] = stored.split(':')
  const actual = await scryptAsync(password, Buffer.from(salt, 'hex'), 64)
  return timingSafeEqual(actual, Buffer.from(hash, 'hex'))
}

const credentials = {
  type: 'object',
  required: ['email', 'password'],
  additionalProperties: false,
  properties: {
    email: { type: 'string', format: 'email', maxLength: 254 },
    password: { type: 'string', minLength: 10, maxLength: 200 },
  },
}
const authLimit = { rateLimit: { max: 10, timeWindow: '1 minute' } }

export async function buildApp(deps: { db: Db; jwtSecret: string; webhookSecret: string; logger?: boolean }) {
  const { db } = deps
  const app = Fastify({ logger: deps.logger ?? false })
  await app.register(fastifyJwt, { secret: deps.jwtSecret, sign: { expiresIn: '1h' } })
  // ponytail: counters live in this process. Give the plugin a Redis store before running more than one replica.
  await app.register(rateLimit, { max: 300, timeWindow: '1 minute' })

  app.addContentTypeParser('application/json', { parseAs: 'string' }, (request, body, done) => {
    request.rawBody = body as string
    try {
      done(null, body ? JSON.parse(body as string) : {})
    } catch {
      done(new AppError(400, 'invalid JSON'))
    }
  })

  app.setErrorHandler((error: { statusCode?: number; message: string }, request, reply) => {
    const status = error.statusCode ?? 500
    if (status >= 500) request.log.error(error)
    reply.status(status).send({ error: status >= 500 ? 'internal error' : error.message })
  })

  const requireRole = (role?: Role) => async (request: FastifyRequest) => {
    await request.jwtVerify()
    if (role && request.user.role !== role) throw new AppError(403, 'forbidden')
  }

  app.get('/healthz', async () => ({ ok: true }))
  app.get('/readyz', async (_request, reply) => {
    try {
      await db.client.db().command({ ping: 1 })
      return { ok: true }
    } catch {
      return reply.status(503).send({ ok: false })
    }
  })

  app.post<{ Body: { email: string; password: string } }>(
    '/auth/register',
    { schema: { body: credentials }, config: authLimit },
    async (request, reply) => {
      const user = {
        _id: randomUUID(),
        email: request.body.email.toLowerCase(),
        passwordHash: await hashPassword(request.body.password),
        role: 'customer' as const,
      }
      try {
        await db.users.insertOne(user)
      } catch (error) {
        if (isDuplicateKey(error)) throw new AppError(409, 'email is already registered')
        throw error
      }
      return reply.status(201).send({ id: user._id, token: app.jwt.sign({ sub: user._id, role: user.role }) })
    },
  )

  app.post<{ Body: { email: string; password: string } }>(
    '/auth/login',
    { schema: { body: credentials }, config: authLimit },
    async (request) => {
      const user = await db.users.findOne({ email: request.body.email.toLowerCase() })
      if (!user || !(await verifyPassword(request.body.password, user.passwordHash))) {
        throw new AppError(401, 'invalid credentials')
      }
      return { token: app.jwt.sign({ sub: user._id, role: user.role }) }
    },
  )

  app.get<{ Querystring: { status?: VehicleStatus; after?: string; limit: number } }>(
    '/vehicles',
    {
      preHandler: requireRole(),
      schema: {
        querystring: {
          type: 'object',
          properties: {
            status: { type: 'string', enum: ['available', 'reserved', 'subscribed', 'maintenance'] },
            after: { type: 'string', maxLength: 64 },
            limit: { type: 'integer', minimum: 1, maximum: 100, default: 20 },
          },
        },
      },
    },
    async (request) => {
      const { status, after, limit } = request.query
      const items = await db.vehicles
        .find({ ...(status && { status }), ...(after && { _id: { $gt: after } }) })
        .sort({ _id: 1 })
        .limit(limit)
        .toArray()
      return { items, next: items.length === limit ? items[items.length - 1]._id : null }
    },
  )

  app.post<{ Body: { plate: string; model: string; monthlyPriceCents: number } }>(
    '/vehicles',
    {
      preHandler: requireRole('ops'),
      schema: {
        body: {
          type: 'object',
          required: ['plate', 'model', 'monthlyPriceCents'],
          additionalProperties: false,
          properties: {
            plate: { type: 'string', minLength: 1, maxLength: 20 },
            model: { type: 'string', minLength: 1, maxLength: 80 },
            monthlyPriceCents: { type: 'integer', minimum: 1 },
          },
        },
      },
    },
    async (request, reply) => {
      const vehicle = { _id: randomUUID(), ...request.body, status: 'available' as const }
      try {
        await db.vehicles.insertOne(vehicle)
      } catch (error) {
        if (isDuplicateKey(error)) throw new AppError(409, 'plate is already registered')
        throw error
      }
      return reply.status(201).send(vehicle)
    },
  )

  app.post<{ Body: { vehicleId: string }; Headers: { 'idempotency-key': string } }>(
    '/subscriptions',
    {
      preHandler: requireRole('customer'),
      schema: {
        headers: {
          type: 'object',
          required: ['idempotency-key'],
          properties: { 'idempotency-key': { type: 'string', minLength: 8, maxLength: 128 } },
        },
        body: {
          type: 'object',
          required: ['vehicleId'],
          additionalProperties: false,
          properties: { vehicleId: { type: 'string', minLength: 1, maxLength: 64 } },
        },
      },
    },
    async (request, reply) => {
      const subscription = await createSubscription(db, {
        customerId: request.user.sub,
        vehicleId: request.body.vehicleId,
        idempotencyKey: request.headers['idempotency-key'],
      })
      return reply.status(201).send(subscription)
    },
  )

  async function ownedSubscription(request: FastifyRequest<{ Params: { id: string } }>) {
    const subscription = await db.subscriptions.findOne({ _id: request.params.id })
    if (!subscription || (request.user.role !== 'ops' && subscription.customerId !== request.user.sub)) {
      throw new AppError(404, 'subscription not found')
    }
    return subscription
  }

  app.get<{ Params: { id: string } }>('/subscriptions/:id', { preHandler: requireRole() }, ownedSubscription)

  app.post<{ Params: { id: string } }>(
    '/subscriptions/:id/cancel',
    { preHandler: requireRole() },
    async (request) => cancelSubscription(db, (await ownedSubscription(request))._id),
  )

  app.post<{ Params: { id: string }; Body: { reason: string } }>(
    '/vehicles/:id/maintenance',
    {
      preHandler: requireRole('ops'),
      schema: {
        body: {
          type: 'object',
          required: ['reason'],
          additionalProperties: false,
          properties: { reason: { type: 'string', minLength: 1, maxLength: 200 } },
        },
      },
    },
    async (request, reply) =>
      reply.status(201).send(await startMaintenance(db, request.params.id, request.body.reason)),
  )

  app.post<{ Params: { id: string } }>(
    '/maintenance/:id/complete',
    { preHandler: requireRole('ops') },
    async (request) => completeMaintenance(db, request.params.id),
  )

  app.get('/ops/dashboard', { preHandler: requireRole('ops') }, async () => dashboard(db))

  app.post<{ Body: { id: string; type: string; data: { chargeId: string; idempotencyKey: string } } }>(
    '/webhooks/payments',
    {
      preValidation: async (request) => {
        if (!verifyWebhook(request.rawBody ?? '', request.headers['x-signature'], deps.webhookSecret)) {
          throw new AppError(401, 'invalid signature')
        }
      },
      schema: {
        body: {
          type: 'object',
          required: ['id', 'type', 'data'],
          properties: {
            id: { type: 'string', minLength: 1, maxLength: 128 },
            type: { type: 'string', enum: ['charge.succeeded', 'charge.failed'] },
            data: {
              type: 'object',
              required: ['chargeId', 'idempotencyKey'],
              properties: {
                chargeId: { type: 'string', minLength: 1, maxLength: 128 },
                idempotencyKey: { type: 'string', pattern: '^[0-9a-f-]{36}:[0-9]{1,3}$' },
              },
            },
          },
        },
      },
    },
    async (request) => {
      const { id, type, data } = request.body
      const outcome = await applyCharge(
        db,
        { ...data, status: type === 'charge.succeeded' ? 'succeeded' : 'failed' },
        { eventId: id },
      )
      return { outcome }
    },
  )

  return app
}
