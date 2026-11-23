import { MongoMemoryReplSet } from 'mongodb-memory-server'

export default async function setup() {
  const replicaSet = await MongoMemoryReplSet.create({ replSet: { count: 1 } })
  process.env.MONGO_TEST_URL = replicaSet.getUri()
  return async () => {
    await replicaSet.stop()
  }
}
