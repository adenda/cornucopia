# Cornucopia
A microservice for auto-sharding Redis Cluster.

Implemented on Kafka and Akka Streams, with Salad wrapping the Java 8 Lettuce API for Redis.

# Operations
The following keys for Kafka messages correspond to operations to be performed in cornucopia.

### Add or Remove a Node
* +master 
* +slave
* -master
* -slave

The value contained in the message is the URI of the node to operate on.

New master nodes will be assigned hash slots based on the assumption that CPU usage is balanced among the nodes (which should be the case if your application partitions the data using a good hash key).

Some attempt will be made to reassign slots in a way that minimizes data migration.

New slave nodes will initially be assigned to the master with the least slaves.
Beyond that, Redis Cluster itself has the ability to migrate slaves to other masters based on the cluster configuration.

# Auto-Scaling
High memory utilization will require more master nodes.
High master CPU utilization (writes) will require more master nodes.
High slave CPU utilization (reads) will require more slave nodes.

Determination of scaling requirements is outside the scope of this project.
A Kubernetes project will be available to request node types based on cluster utilization.
