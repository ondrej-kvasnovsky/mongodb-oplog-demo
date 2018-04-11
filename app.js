const OplogService = require('./oplog-service')
const oplogService = new OplogService()
;(async () => {
  await oplogService.start()
})().catch(err => {
  console.error(err)
})
