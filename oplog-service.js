const fs = require('fs')
const Rx = require('rxjs/Rx')

const MongoClient = require('mongodb').MongoClient
const Timestamp = require('mongodb').Timestamp
const BSON = require('bson')
const url = 'mongodb://localhost:27017,127.0.0.1:27018/?replicaSet=rs0'
const oplogDbName = 'local'
const dbName = 'test'
const collectionName = 'items'
const lastIdFileName = 'last_timestamp.txt'

module.exports = class {
  async start() {
    const client = await MongoClient.connect(url)
    console.log('Connected!')
    try {
      const db = await client.db(oplogDbName)
      const collection = await db.collection(collectionName)

      // this.readExisting(collection)
      this.startTailingOplog(db)
    } catch (e) {
      console.log(e)
    }
  }

  async readExisting(collection) {
    // const lastItem = await collection
    //   .find()
    //   .sort({ $natural: 1 })
    //   .limit(1)
    //   .next()
    // console.log('last item')
    // console.log(lastItem)
    // {_id: {$gt: lastItem._id}}
    const cursor = await collection.find().batchSize(10)
    console.log('Reading existing items...')
    const observable = Rx.Observable.create(async observer => {
      while (await cursor.hasNext()) {
        observer.next(await cursor.next())
      }
      cursor.close()
      observer.complete()
    }).bufferCount(10)
    const subscription = observable.subscribe(
      value => {
        console.log('Push existing items to Firehose', value)
      },
      err => {
        console.log(err)
      },
      async () => {
        console.log('this is the end')
        subscription.unsubscribe()
      }
    )
    setTimeout(() => {
      console.log('Timed out, unsubscribe!')
      subscription.unsubscribe()
    }, 24 * 60 * 60 * 1000)
  }

  async startTailingOplog(db) {
    const filter = {
      ts: {
        $gt: await this.getLastTimestamp()
      },
      ns: `${dbName}.${collectionName}`
    }

    const oplog = await db.collection('oplog.rs')
    const options = {
      projection: { ts: 1, o: 1 }
    }
    const cursor = await oplog
      .find(filter, options)
      .batchSize(100)
      // .addCursorFlag('oplogReplay', true) // we can use projection when this is on
      .addCursorFlag('noCursorTimeout', true)
      .addCursorFlag('tailable', true)
      .addCursorFlag('awaitData', true)

    // 'no callbacks' way to tail oplog
    // let hasNext = await cursor.hasNext()
    // while (hasNext) {
    //   const message = await cursor.next()
    //   console.log(message.ns)
    //   console.log(`${dbName}.${collectionName}`)
    //   console.log(message)
    //   const lowBits = message.ts.getLowBits()
    //   const highBits = message.ts.getHighBits()
    //   console.log(new Timestamp(lowBits, highBits))
    //   hasNext = await cursor.hasNext()
    // }

    const observable = Rx.Observable.create(async observer => {
      let hasNext = await cursor.hasNext()
      while (hasNext) {
        const message = await cursor.next()
        observer.next(message)
        hasNext = await cursor.hasNext()
      }
    }).bufferCount(100)
    const subscription = observable.subscribe(
      value => {
        console.log('Reading oplog', value)
        const low = value[value.length - 1].ts.getLowBits()
        const high = value[value.length - 1].ts.getHighBits()
        fs.writeFileSync(lastIdFileName, low + ':' + high)
      },
      err => {
        console.log(err)
      },
      async () => {
        console.log('this is the end!')
        subscription.unsubscribe()
      }
    )
  }

  async getLastTimestamp() {
    let timestamp = new Timestamp(0, 0)
    if (fs.existsSync(lastIdFileName)) {
      const text = await fs.readFileSync(lastIdFileName, 'utf-8')
      const array = text.split(':')
      timestamp = new Timestamp(array[0], array[1])
    }
    return timestamp
  }
}
