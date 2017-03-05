# Fault Tolerant and Scalable Key-value Store

### Due : Sunday, March 19th, 11:59pm


In previous homework you built a scalable key-value store: the data can be partitioned across several nodes. We always can add more partitions to increase the capacity of a key-value store.
In this homework you extend the scalable key-value store to be fault tolerant, available and eventually consistent.

### Partition Tolerance, Availability and Consistency
Your key-value store should be partition tolerant: it will continue functioning in the face of network partitions and node failure. You might look into the following docker commands "docker network disconnect" and "docker network connect" to learn about adding and removing nodes from a network.     

Due to the CAP theorem, we know that we cannot have a fault tolerant key-value store that is both available and strongly consistent. In this assignment, we favour availability over strong consistency. Your key-value store should always return responses to requests, even if it cannot guarantee the most recent data.

Even though we cannot guarantee strong consistency, it is possible to guarantee eventual consistency and convergence. Right after a network is healed, the key-value store can return stale data. However, you key-values store should guarantee that the data is up-to date after a certain time bound. In other words, the key-value store should have the property of bounded staleness. The time bound for this assignment is 5 seconds.

### Conflict Resolution
It possible that after a network is healed, two nodes end up with different values for the same key. Such a conflict should be resolved using causal order, if it can be established. If the events are causally concurrent, then the tie should be resolved using local time stamps on replica nodes. Note that your key-value store needs to have a mechanism to establish a causal relation between events. 

### Starting Key-value Store
To start a key value store we use the following environmental variables. 

* "K" is the number of replicas per partition. Each partition owns a subset of keys.
* "VIEW" is the list of ip:ports pairs of nodes.

An example of starting a key-value store with 4 nodes and partition size 2:

```
docker run -p 8081:8080 --ip=10.0.0.21 --net=mynet -e K=2 -e VIEW="10.0.0.21:8080,10.0.0.22:8080,10.0.0.23:8080,10.0.0.24:8080" mycontainer
docker run -p 8082:8080 --ip=10.0.0.22 --net=mynet -e K=2 -e VIEW="10.0.0.21:8080,10.0.0.22:8080,10.0.0.23:8080,10.0.0.24:8080" mycontainer
docker run -p 8083:8080 --ip=10.0.0.23 --net=mynet -e K=2 -e VIEW="10.0.0.21:8080,10.0.0.22:8080,10.0.0.23:8080,10.0.0.24:8080" mycontainer
docker run -p 8084:8080 --ip=10.0.0.24 --net=mynet -e K=2 -e VIEW="10.0.0.21:8080,10.0.0.22:8080,10.0.0.23:8080,10.0.0.24:8080" mycontainer
```

## API
### Key-value Operations
GET, PUT requests are a bit different from the previous assignments. Now they return extra information about partitions and causal order. The information used to establish causal order is stored in "causal_payload" and "timestamp" fields. The "causal_payload" field is used to establish causality between events. For example, if a node performs a write A followed by write B, then the corresponding causal payloads X_a and X_b should satisfy inequality X_a < X_b.  Similarly, a causal payload X of a send event should be smaller that the causal payload Y of the corresponding receive event, i.e. X < Y. The value of the "causal_payload" field is solely depends on the mechanism you use to establish the causal order. The value of the "timestamp" field is the wall clock time on the replica that first processed the write.

To illustrate, let a client A writes a key, and a client B reads that key and then writes it, B's write should replace A's write (even if it lands on a different server). To make sure that this works, we will always pass the causal payload of previous reads into future writes. You must ensure that B's read returns a causal payload higher than the payload associated with A's write!

To consider another example, let 2 clients write concurrently to 2 different nodes respectively. And let T_1 and T_2 be the corresponding write timestamps measured according to the nodes' wall clocks. If T_1 > T_2 then the first write wins. If T_1 < T_2 then the second write wins. However, how can we resolve the writing conflict if T_1 == T_2? Can we use the identity of the nodes?


* A GET request to "/kvs/\<key\>" retrieves the value that corresponds to the key; a response object has the following fields: "msg", "value", "partition_id", "causal_payload", "timestamp". A response to a successful request looks like this
```
{
    "msg":"success",
    "value":1,
    "partition_id": 3,
    "causal_payload": "1.0.0.4",
    "timestamp": "1256953732"
}
```

* A PUT request to "/kvs/\<key\>" with the data fields "val=\<value\>" and "causal_payload=\<causal_payload\>" creates a record in the key value store. The "causal_payload" data field is the causal payload observed by the client’s previous reads (why we need it? See the example above). If the client did not do any reads, then the  causal payload is an empty string. The response object has the following fields: "msg", "partition_id", "causal_payload", "timestamp". An example of a successful response looks like
```
{
    "msg":"success",
    "partition_id": 3,
    "causal_payload": "1.0.0.4",
    "timestamp": "1256953732"
}
```

* DELETE. You do not need to implement deletion of keys in this assignment.

### Adding and Deleting Nodes
We use "update_view" request to add and delete nodes. An addition or a deletion of a node might change the number of partitions. For example, let we started a key value-store with 6 nodes and partition size K=3. It follows that the key-value store has 2 partitions with 3 replicas each. If we add a new node, then due to the partition size constraint, the number of partitions becomes 3.

* A PUT request on "/kvs/update_view?type=add" with the data payload "ip_port=\<ip_port\>" adds the node to the key-value store. It increments the number of partitions, if needed. A successful response returns the partition id of the new node, and the total number of partitions. It should look like:
```
{
    "msg":"success",
    "partition_id": 2,
    "number_of_partitions":3,
}
```
* A PUT request on "/kvs/update_view?type=remove" with the payload "ip_port=\<ip_port\>" removes the node. It decrements the number of partitions, if needed. A successful response object contains the total number of partitions after the deletion, for example
```
{
    "msg":"success",
    "number_of_partitions": 2
}
```

### Obtaining Partition Information
The following methods allow a client to obtain information about partitions.

* A GET request on "/kvs/get_partition_id" returns the partition id where the node belongs to. For example, the following curl request `curl -X GET http://localhost:8083/kvs/get_partition_id` returns the id of the node that we can access via `localhost:8083`. A successful response looks like
```
{
    "msg":"success",
    "partition_id": 3,
}
```

* A GET request on "/kvs/get_all_partition_ids" returns a list with ids of all partitions.
```
{
    "msg":"success",
    "partition_id_list": [0,1,2,3]
}
```

* A GET request on "/kvs/get_partition_members" with data payload "partition_id=\<partition_id\>" returns a list of nodes in the partition.
    For example the following curl request `curl -X GET http://localhost:8083/kvs/get_partition_members -d 'partition_id=1' ` will return a list of nodes in the partition with id 1.
```
{
    "msg":"success",
    "partition_members": ["10.0.0.21:8080", "10.0.0.22:8080", "10.0.0.23:8080"]
}
```

### Error Responses
All error responses contain 2 fields "msg" and "error". The error field contains the details about the error, for example
```
{
    "msg":"error",
    "error":"key value store is not available"
}
```
