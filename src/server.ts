import { buildApp } from './app.ts'
import { config } from './config.ts'
import { connect } from './db.ts'

const db = await connect(config.mongoUrl)
const app = await buildApp({ db, jwtSecret: config.jwtSecret, webhookSecret: config.webhookSecret, logger: true })
await app.listen({ port: config.port, host: '0.0.0.0' })

for (const signal of ['SIGTERM', 'SIGINT'] as const) {
  process.once(signal, async () => {
    await app.close()
    await db.client.close()
  })
}
