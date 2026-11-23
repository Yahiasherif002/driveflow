function required(name: string): string {
  const value = process.env[name]
  if (!value) throw new Error(`${name} is required`)
  return value
}

export const config = {
  port: Number(process.env.PORT ?? 3000),
  mongoUrl: process.env.MONGO_URL ?? 'mongodb://localhost:27017/?replicaSet=rs0',
  kafkaBrokers: (process.env.KAFKA_BROKERS ?? 'localhost:29092').split(','),
  get jwtSecret() {
    return required('JWT_SECRET')
  },
  get webhookSecret() {
    return required('WEBHOOK_SECRET')
  },
}
