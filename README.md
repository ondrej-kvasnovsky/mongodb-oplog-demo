# MongoDB Oplog Demo

Connects to MongoDB collection and reads oplog (operation logs) collection. 
Is able to resume using the latest timestamp.  

## Requirements

* nodejs
* npm
* mongodb

## MongoDB ReplicaSet

MongoDB needs to be started in replica set. 
Use `mongo-replicaset/setup.sh` to create mongodb using replica set.

After you ran `sh setup.sh`, you need to run: 
```
rs.initiate({_id:"rs0", members: [{_id:0, host:"127.0.0.1:27017", priority:100}, {_id:1, host:"127.0.0.1:27018", priority:50}, {_id:2, host:"127.0.0.1:27019", arbiterOnly:true}]})
use test
db.createCollection('items')
```

Connect to database and create `test` database and `items` collection in it.

## How to run the demo

Start the app.js using npm script. 
Then insert some items into `test.items` collection.

```
$ npm install
$ npm start
```
