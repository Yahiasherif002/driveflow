# DriveFlow

Backend for a car subscription and fleet platform: customers subscribe to a vehicle monthly, pay recurring charges, and cancel; operations manage availability and maintenance.

The project is deliberately small in surface area and deep in failure handling. It is a modular monolith (one API process, one worker process) written in TypeScript on Node.js 24, with MongoDB transactions and Kafka.

## What it guarantees

| Failure | Guarantee | How |
| --- | --- | --- |
| Two customers book the same car at the same instant | Exactly one wins | Conditional `findOneAndUpdate` on the vehicle inside a transaction, backed by a partial unique index (one live subscription per vehicle) |
| A client retries a booking request | Same subscription returned, nothing duplicated | `Idempotency-Key` header with a unique index per customer |
| The provider charges a card but the response is lost | The retry never charges twice | Charge idempotency key is `paymentId:attempt`; the attempt counter only advances once a result is saved |
| The service crashes before recording a successful charge | The payment is recovered | Reconciliation replays the provider's charges through the same idempotent apply step |
| A webhook is delivered twice or forged | Applied once; forged requests rejected | HMAC-SHA256 signature check, event-id dedupe in the same transaction as the state change |
| A card keeps declining | Exponential backoff, then dead-letter | 4 attempts (1h, 2h, 4h backoff); the payment is marked `dead`, the subscription expires, the car returns to the fleet |
| Money arrives for a subscription that already ended | Flagged for refund | The payment is marked `refundRequired` and a `RefundRequired` event is emitted |
| The broker is down when a subscription is created | No event is lost | Transactional outbox: the event is written in the same transaction as the business change and relayed to Kafka with retries |
| Kafka redelivers an event | The consumer effect runs once | Processed-event ledger written in the same transaction as the effect |
| A consumer cannot process a message | The partition is not blocked | 3 attempts, then the message goes to `driveflow.events.dlq` |

## Subscription lifecycle

```
pending  --payment confirmed-->  active  --payment failed-->  past_due
pending  --hold expired------->  expired                      past_due --recovered--> active
active / past_due / pending --cancel--> cancelled             past_due --retries exhausted--> expired
```

Transitions live in one table (`TRANSITIONS` in `src/domain.ts`) and are enforced by a conditional database update, so an illegal or concurrent transition cannot be written.

## Architecture

```
client -> Fastify API -> MongoDB (replica set, transactions)
                            |  outbox collection
                         worker: outbox relay -> Kafka -> notification consumer -> DLQ
                         worker: billing, hold expiry, reconciliation
```

- `src/subscriptions.ts`: booking, cancellation, hold expiry, state transitions
- `src/billing.ts`: recurring billing, retries, webhooks, reconciliation
- `src/outbox.ts`, `src/kafka.ts`: transactional outbox, relay, idempotent consumer, DLQ
- `src/fleet.ts`: maintenance and the operations dashboard
- `src/app.ts`: HTTP API with JWT auth, roles, validation, rate limiting, health checks

## Run it

```sh
cp .env.example .env          # then set two long random secrets
docker compose up -d --build
docker compose run --rm api node src/seed.ts ops@example.com 'a-long-password'
```

The API listens on `http://localhost:3000`.

| Endpoint | Role | Purpose |
| --- | --- | --- |
| `POST /auth/register`, `POST /auth/login` | public | Get a JWT |
| `GET /vehicles?status=available&limit=20&after=<id>` | any | Cursor-paginated fleet list |
| `POST /subscriptions` with `Idempotency-Key` | customer | Reserve a vehicle and open the first payment |
| `GET /subscriptions/:id`, `POST /subscriptions/:id/cancel` | owner or ops | Read or cancel |
| `POST /vehicles`, `POST /vehicles/:id/maintenance`, `POST /maintenance/:id/complete` | ops | Fleet management |
| `GET /ops/dashboard` | ops | Availability, utilization, overdue payments, renewals due |
| `POST /webhooks/payments` with `X-Signature` | provider | Signed payment events |
| `GET /healthz`, `GET /readyz` | public | Liveness and readiness |

## Tests

```sh
npm ci
npm run typecheck
npm test                                   # 27 tests, in-memory MongoDB replica set
KAFKA_BROKERS=localhost:29092 npm test     # also runs the Kafka round trip (needs docker compose up)
```

The suite includes a test that fires 200 concurrent booking requests at one vehicle and asserts that exactly one subscription, one payment, and one outbox event exist afterwards.

## Known limits

- The payment provider is a simulator (`FakeProvider`). A real gateway plugs in behind the `PaymentProvider` interface.
- Notifications are recorded in a collection, not sent by email.
- Rate limiting is per process; it needs a shared store before running several API replicas.
- Plan upgrades, refunds execution, metrics, and tracing are not implemented.
