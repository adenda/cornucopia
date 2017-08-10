# Cornucopia

Cornucopia is a controller for Redis cluster that performs auto-sharding when adding and removing Redis cluster nodes.

This project is originally a fork from [`kliewkliew/cornucopia`](https://github.com/kliewkliew/cornucopia).

## Operations

The following keys for task messages correspond to operations to be performed in Cornucopia.

### Add or Remove a Node

* `+master`: Add Master node
* `+slave`: Add Slave node
* `-master`: Remove Master node
* `-slave`: Remove Slave node

The value contained in the message is the URI of the node to operate on. See [Redis URI and connection details](https://github.com/mp911de/lettuce/wiki/Redis-URI-and-connection-details). When adding a node to the cluster, the URI value indicates the Redis cluster node to be added to the cluster. When removing a node from the cluster, the URI value indicates the cluster node to be removed. If the cluster node to be removed is not of the node type (master or slave) indicated in the task message, then the cluster node is converted into that node type by performing a manual failover. Cornucopia does not support removing a slave node from the cluster if the URI value in the task message hosts a master node that is not replicated by a slave node. Technically this is a limitation. That said, it could be considered a best practice to have all master nodes replicated by a slave at any given time in the cluster.

Adding or removing a master node will automatically trigger a cluster reshard event. 

New slave nodes will initially be assigned to the master with the least slaves. Beyond that, Redis Cluster itself has the ability to migrate slaves to other masters based on the cluster configuration.

Note that Redis cluster will automatically assign or reassign nodes between master or slave roles, or migrate slaves between masters or do failover.
You may see errors due to Redis doing reassignment when the cluster is small.
For example, when testing with only two nodes, after adding the second node as a master, the first node can become a slave.
If you then try to remove the second node, there will be no masters left.
The behaviour is more predictable as more nodes are added to the cluster.
It is generally advisable to maintain a cluster with at least three master nodes at all times.

#### Using Cornucopia as a microservice

Cornucopia can be run as a stand-alone microservice. The interface to this microservice is a HTTP Rest interface.

For example, assume that the micro service is running on HTTP port 9001 on localhost (which is the default). To add a new master node to the cluster on localhost:7006, run the following command:

    curl -X POST \
      http://localhost:9001/task2 \
      -H 'content-type: application/json' \
      -d '{
    	"operation": "+master",
    	"redisNodeIp": "redis://localhost:7006"
    }'
    
#### Using Cornucopia as a library in your application

Include Cornucopia in your `build.sbt` file: `"com.adendamedia" %% "cornucopia" % "0.5.0"`. Control messages are sent to Cornucopia using an ActorRef that must be imported.

```scala
import com.adendamedia.cornucopia.Library
import com.adendamedia.cornucopia.actors.Gatekeeper.{Task, TaskAccepted, TaskDenied}
```

From within your own AKKA actor you can send a message to Cornucopia. Note that the library requires an implicit Actor System. This actor system can be reused from within your own application.

```scala
implicit val system: ActorSystem = ActorSystem()
val library: com.adendamedia.cornucopia.Library = new Library
val cornucopiaRef = library.ref    
cornucopiaRef ! Task("+master", "redis://localhost:7006")
```

## Application configuration

### Cornucopia configuration settings

| Setting  | Description  |
|:----------|:--------------|
| `cornucopia.http.host` | The hostname where the Cornucopia microservice is run (default: localhost). |
| `cornucopia.http.port` | The port on which the Cornucopia microservice is run (default: 9001). |
| `cornucopia.migrate.slots.workers` | The number of workers to run migrate slot commands during cluster resharding. This setting effectively rate limits the number of asynchronous migrate slots jobs that can be running at any given time. (default: 5) |

### Redis configuration settings

| Setting  | Description  |
|:----------|:--------------|
| `redis.cluster.seed.server.host` | Initial node-hostname from which the full cluster topology will be derived (default: localhost). |
| `redis.cluster.seed.server.port` | Initial node-port from which the full cluster topology will be derived (default: 7000). |

