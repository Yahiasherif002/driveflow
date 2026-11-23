import { randomUUID } from 'node:crypto'
import { hashPassword } from './app.ts'
import { config } from './config.ts'
import { connect } from './db.ts'

const [email, password] = process.argv.slice(2)
if (!email || !password || password.length < 10) {
  console.error('usage: npm run seed -- <ops-email> <password of 10+ characters>')
  process.exit(1)
}

const db = await connect(config.mongoUrl)
await db.users.updateOne(
  { email: email.toLowerCase() },
  { $set: { passwordHash: await hashPassword(password), role: 'ops' }, $setOnInsert: { _id: randomUUID() } },
  { upsert: true },
)
const models = ['Toyota Corolla', 'Nissan Sunny', 'Kia Sportage', 'Hyundai Tucson', 'MG ZS']
for (let i = 1; i <= 20; i++) {
  await db.vehicles.updateOne(
    { plate: `DXB-${1000 + i}` },
    {
      $setOnInsert: {
        _id: randomUUID(),
        model: models[i % models.length],
        monthlyPriceCents: 150_000 + (i % models.length) * 25_000,
        status: 'available',
      },
    },
    { upsert: true },
  )
}
console.log(`seeded ops user ${email} and 20 vehicles`)
await db.client.close()
