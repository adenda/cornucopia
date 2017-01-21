# Cornucopia
A microservice for auto-sharding Redis Cluster.

Master branch is for development.
Stable branch is for production.

Implemented on Kafka and Akka Streams, with Salad wrapping the Java 8 Lettuce API for Redis.

# Operations
The following keys for Kafka messages correspond to operations to be performed in cornucopia.

## Add or Remove a Node
* `+master`
* `+slave`
* `-node`

The value contained in the message is the URI (must be resolvable at the microservice) of the node to operate on.
https://github.com/mp911de/lettuce/wiki/Redis-URI-and-connection-details

ie. (ensuring that the version of the binaries match the broker version)
```
/opt/kafka_2.11-0.10.1.0/bin/kafka-console-producer.sh \
--topic cornucopia \
--broker-list kafka-broker-1-vm:9092,kafka-broker-2-vm:9092 \
--property parse.key=true \
--property key.separator=","
> +master,redis://redis-seed-2
```

Adding or removing a master node will automatically trigger a `*reshard` event.

New slave nodes will initially be assigned to the master with the least slaves.
Beyond that, Redis Cluster itself has the ability to migrate slaves to other masters based on the cluster configuration.

Note that Redis cluster will automatically assign or reassign nodes between master or slave roles, or migrate slaves between masters or do failover.
You may see errors due to Redis doing reassignment when the cluster is small.
ie. When testing with only two nodes, after adding the second node as a master, the first node can become a slave.
If you then try to remove the second node, there will be no masters left.
The behaviour is more predictable as more nodes are added to the cluster.

There may be multiple node ids (dead nodes that were not previously removed) assigned to one URI and Redis only returns one at a time so you may have remove the same URI multiple times to remove the correct node id.

## Redistribute Hash Slots
* `*reshard`

ie.
```
/opt/kafka_2.11-0.10.1.0/bin/kafka-console-producer.sh \
--topic cornucopia \
--broker-list kafka-broker-1-vm:9092,kafka-broker-2-vm:9092 \
--property parse.key=true \
--property key.separator=","
> *reshard,
```

Hash slots will be assigned to master nodes based on the assumption that CPU usage is balanced among the nodes (which should be the case if your application partitions the data using a good hash key).

Some attempt will be made to reassign slots in a way that minimizes data migration.

If a node is in a bad state, you can run the following command on that node:
```
redis-cli cluster reset
```

# Auto-Scaling
High memory utilization will require more master nodes.
High master CPU utilization (writes) will require more master nodes.
High slave CPU utilization (reads) will require more slave nodes.

Determination of scaling requirements is outside the scope of this project.
A Kubernetes project will be available to instantiate and request initialization for node types based on cluster utilization.
