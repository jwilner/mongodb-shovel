# mongodb-shovel
A simple shovel for republishing events from MongoDB capped collections to RabbitMQ for dispatch

## what?
MongoDB's oplog (and capped collections more generally) is ripe for driving an event bus. The idea behind this shovel is to do one simple thing -- [tail a capped collection](https://docs.mongodb.com/manual/core/tailable-cursors/) and shovel matching results into a message broker (I'll do [RabbitMQ](https://www.rabbitmq.com/) first). 

In order to provide basic ordering guarantees through to the broker, this will eschew any real concurrency; multiple shovels could be run simultaneously to achieve concurrency while still preserving the ordering guarantees for the individual queries.
