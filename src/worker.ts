import { FakeProvider, reconcile, runBilling } from './billing.ts'
import { config } from './config.ts'
import { connect } from './db.ts'
import { createKafka, createPublisher, ensureTopics, runConsumer } from './kafka.ts'
import { recordNotification, relayOnce } from './outbox.ts'
import { expireHolds } from './subscriptions.ts'

const db = await connect(config.mongoUrl)
const kafka = createKafka(config.kafkaBrokers)
await ensureTopics(kafka)
const publisher = await createPublisher(kafka)
const stopConsumer = await runConsumer(kafka, 'notifications', (event) => recordNotification(db, event))
// ponytail: simulated provider with in-memory charges. Implement PaymentProvider for a real gateway to go live.
const provider = new FakeProvider()

function every(ms: number, job: string, run: () => Promise<unknown>): NodeJS.Timeout {
  let running = false
  return setInterval(async () => {
    if (running) return
    running = true
    try {
      await run()
    } catch (error) {
      console.error(JSON.stringify({ level: 'error', job, error: String(error) }))
    } finally {
      running = false
    }
  }, ms)
}

const timers = [
  every(1_000, 'outbox-relay', () => relayOnce(db, publisher.publish)),
  every(10_000, 'billing', () => runBilling(db, provider)),
  every(30_000, 'hold-expiry', () => expireHolds(db)),
  every(60_000, 'reconciliation', () => reconcile(db, provider)),
]
console.log(JSON.stringify({ level: 'info', msg: 'worker started' }))

for (const signal of ['SIGTERM', 'SIGINT'] as const) {
  process.once(signal, async () => {
    timers.forEach(clearInterval)
    await stopConsumer()
    await publisher.close()
    await db.client.close()
  })
}
