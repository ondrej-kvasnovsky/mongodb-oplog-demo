const fs = require('fs')
const Rx = require('rxjs/Rx')

const MongoClient = require('mongodb').MongoClient
const BSON = require('bson')
const url = 'mongodb://localhost:27017,127.0.0.1:27018/?replicaSet=rs0'
const dbName = 'local'
const lastIdFileName = 'restore_object.bson'

module.exports = class {
  async start() {
    const client = await MongoClient.connect(url)
    console.log('Connected!')
    try {
      const db = await client.db(dbName)
      const collection = await db.collection('items')

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
    console.log('1')
    const oplog = await db.collection('oplog.rs')
    const cursor = await oplog
      .find({}) // { ts: { $gt: 1 }}
      .batchSize(5)
      .addCursorFlag('oplogReplay', true)
      .addCursorFlag('noCursorTimeout', true)
      .addCursorFlag('tailable', true)
      .addCursorFlag('awaitData', true)
      .addCursorFlag('numberOfRetries', 10)

    let hasNext = await cursor.hasNext()
    console.log(hasNext)
    while (hasNext) {
      console.log(await cursor.next())
      hasNext = await cursor.hasNext()
    }

    console.log('2')
    const query = {
      ts: { $gt: 1 }
    }
    const options = {
      tailable: true,
      awaitdata: true,
      oplogReplay: true,
      numberOfRetries: -1
    }
    db
      .collection('oplog.rs')
      .find(query, options)
      .stream()
      .on('data', data => console.log(data))
      .on('end', () => {
        console.log('Tailable cursor was ended')
      })
      .on('close', () => {
        console.log('Tailable cursor was closed')
      })
    // {
    //   ts: 1
    // },
    // {
    //   tailable: true,
    //   awaitdata: true,
    //   oplogReplay: true,
    //   numberOfRetries: -1
    // }
    // stream = cursor.stream()

    // And when data arrives at that stream, print it out
    // stream.on('data', function(oplogdoc) {
    //   console.log(oplogdoc)
    // })

    // const observable = Rx.Observable.create(async observer => {
    //   while (await cursor.hasNext()) {
    //     observer.next(await cursor.next())
    //   }
    //   //cursor.close()
    //   //observer.complete()
    // }).bufferCount(10)
    // const subscription = observable.subscribe(
    //   value => {
    //     console.log('Reading oplog', value)
    //   },
    //   err => {
    //     console.log(err)
    //   },
    //   async () => {
    //     console.log('this is the end!')
    //     subscription.unsubscribe()
    //   }
    // )
  }

  getRecoveryObject() {
    let recoverId = null
    if (fs.existsSync(lastIdFileName)) {
      const lastIdFile = fs.readFileSync(lastIdFileName)
      recoverId = BSON.prototype.deserialize(lastIdFile)
    }
    return recoverId
  }
}
