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
    // one way to tail oplog
    //   const collection = db.collection('oplog.rs')
    //   collection.find({}, {tailable: true}).each(function(err, entry) {
    //       if (err) {
    //           // handle error
    //       } else {
    //           // got a new oplog entry
    //           console.log('--- entry', entry)
    //       }
    //   })

    const filter = {ts: {$gt: new Timestamp(0, 1523402029)}}

    const oplog = await db.collection('oplog.rs')
    const cursor = await oplog
      .find(filter)
      .batchSize(100)
      .addCursorFlag('oplogReplay', true)
      .addCursorFlag('noCursorTimeout', true)
      .addCursorFlag('tailable', true)
      .addCursorFlag('awaitData', true)

    // 'no callbacks' way to tail oplog
    // let hasNext = await cursor.hasNext()
    // while (hasNext) {
    //   const message = await cursor.next()
    //   console.log(message.ns)
    //   console.log(`${dbName}.${collectionName}`)
    //   if (message.ns === `${dbName}.${collectionName}`) {
    //     console.log(message)
    //     const lowBits = message.ts.getLowBits()
    //     const highBits = message.ts.getHighBits()
    //     console.log(new Timestamp(lowBits, highBits))
    //   } else {
    //     console.log(new Date() + ' rubbish')
    //   }
    //   hasNext = await cursor.hasNext()
    // }

    // not able to get this approach working...
    // const query = {
    //   ts: { $gt: -1 }
    // }
    // const options = {
    //   tailable: true,
    //   awaitdata: true,
    //   oplogReplay: true,
    //     numberOfRetries: -1
    // }
    // await db.collection('oplog.rs')
    //   .find(query, options)
    //   .stream()
    //   .on('data', data => console.log(data))
    //   .on('end', () => {
    //     console.log('Tailable cursor was ended')
    //   })
    //   .on('close', () => {
    //     console.log('Tailable cursor was closed')
    //   })

    const observable = Rx.Observable.create(async observer => {
      let hasNext = await cursor.hasNext()
      while (hasNext) {
        const message = await cursor.next()
        if (message.ns === `${dbName}.${collectionName}`) {
          observer.next(message)
        } else {
          console.log(new Date() + ' rubbish')
        }
        hasNext = await cursor.hasNext()
      }
    }).bufferCount(100)
    const subscription = observable.subscribe(
      value => {
        console.log('Reading oplog', value)
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

  getLastTimestamp() {
    let timestamp = null
    if (fs.existsSync(lastIdFileName)) {
      const text = fs.readFileSync(lastIdFileName)
      timestamp = text.split(':')
    }
    return timestamp
  }
}
