# Cornucopia

A microservice and library for auto-sharding Redis Cluster. Implemented on Akka Streams, with Salad wrapping the Java 8 Lettuce API for Redis. 

This project is originally a fork from [`kliewkliew/cornucopia`](https://github.com/kliewkliew/cornucopia).

## Operations

The following keys for messages correspond to operations to be performed in Cornucopia.

### Add or Remove a Node

* `+master`
* `+slave`
* `-slave`

The value contained in the message is the URI (must be resolvable at the microservice) of the node to operate on. See [Redis URI and connection details](https://github.com/mp911de/lettuce/wiki/Redis-URI-and-connection-details).

#### Using Cornucopia as a microservice

Cornucopia can be run as a stand-alone microservice. The interface to this microservice is a HTTP Rest interface.

For example, assume that the micro service is running on HTTP port 9001 on localhost (which is the default). To add a new master node to the cluster on localhost:7006, run the following command:

    curl -X POST \
      http://localhost:9001/task \
      -H 'content-type: application/json' \
      -d '{
    	"operation": "+master",
    	"redisNodeIp": "redis://localhost:7006"
    }'
    
#### Using Cornucopia as a library in your appliation

Include Cornucopia in your `build.sbt` file: `"com.adendamedia" %% "cornucopia" % "0.5.0"`. Control messages are sent to Cornucopia using an ActorRef that must be imported.

    import com.adendamedia.cornucopia.Library
    import com.adendamedia.cornucopia.actors.CornucopiaSource.Task
   
Then, for example, from within your own AKKA actor you can send a message to Cornucopia:

    val cornucopiaRef = Library.ref    
    cornucopiaRef ! Task("+master", "redis://localhost:7006")

Adding a master node will automatically trigger a `*reshard` event. Currently, removing a master node is not supported, but should be supported in the next release.

New slave nodes will initially be assigned to the master with the least slaves.
Beyond that, Redis Cluster itself has the ability to migrate slaves to other masters based on the cluster configuration.

Note that Redis cluster will automatically assign or reassign nodes between master or slave roles, or migrate slaves between masters or do failover.
You may see errors due to Redis doing reassignment when the cluster is small.
For example, when testing with only two nodes, after adding the second node as a master, the first node can become a slave.
If you then try to remove the second node, there will be no masters left.
The behaviour is more predictable as more nodes are added to the cluster.

There may be multiple node ids (dead nodes that were not previously removed) assigned to one URI and Redis only returns one at a time so you may have to remove the same URI multiple times to remove the correct node id.

## Application configuration

### Cornucopia configuration settings

| Setting  | Description  |
|:----------|:--------------|
| `cornucopia.refresh.timeout` | Time (seconds) to wait for cluster topology changes to propagate (default: 5 seconds).  |
| `cornucopia.batch.period` | Time (seconds) to wait for batches to accumulate before executing a job (default: 5 seconds). |
| `cornucopia.http.host` | The hostname where the Cornucopia microservice is run (default: localhost). |
| `cornucopia.http.port` | The port on which the Cornucopia microservice is run (default: 9001). |
| `cornucopia.reshard.interval` | Mininum time (seconds) to wait between reshard events (default: 60 seconds). |
| `cornucopia.reshard.timeout` | The maximum upper time limit (seconds) that the cluster must be resharded within without the resharding failing (default: 300 seconds). |
| `cornucopia.migrate.slot.timeout` | The maximum upper time limit (seconds) that a slot must be migrated from one node to another during resharding without slot migration failing. (default: 60 seconds) |

### Redis configuration settings

| Setting  | Description  |
|:----------|:--------------|
| `redis.cluster.seed.server.host` | Initial node-hostname from which the full cluster topology will be derived (default: localhost). |
| `redis.cluster.seed.server.port` | Initial node-port from which the full cluster topology will be derived (default: 7000). |

## Auto-Scaling

High memory utilization will require more master nodes.
High master CPU utilization (writes) will require more master nodes.
High slave CPU utilization (reads) will require more slave nodes.

Determination of scaling requirements is outside the scope of this project.
A Kubernetes project will be available to instantiate and request initialization for node types based on cluster utilization.
