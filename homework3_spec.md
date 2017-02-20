# Homework 3: Scalable Key-Value Store

### Due : Thursday, March 2nd, 11:59pm

The goal of the 3rd homework is to develop a distributed key-value store that can store lots of data: the amount of data that cannot fit into one single machine.

### Specification Updates
**Monday, Feb. 20**: To specify an identity of an instance, we use environmental variable "IPPORT" that is a pair of the ip address and the port separated by a colon.

### An example 
As an illustration, imagine that you were asked to build a simple web search engine that returns a set of pages related to a user query.  One approach is to create a dictionary that maps a word into a list of web pages that contain the word.  Sometimes, this dictionary is called a reverse index.  When a user searches issues a query, say "warriors basketball", the search engine returns an intersection of web pages corresponding to the individual keywords "warriors" and "basketball".
If the reverse index contains entries for "warriors" and "basketball" as shown below, then the resulting pages are 'warriors.com', 'nba.com', ....

Reverse index:

```
    ...
    'warriors' -> ['www.aztec-history.com', 'nba.com', 'warriors.com', ...]
    'basketball' -> ['nba.com', 'warriors.com', ... ]
    ...
```


If the reverse index contains information about the whole WWW, then there is no hope to store it all in one single machine.

### Scalable Key-Value Store
How can we distribute a key-value store across several machines? There are many strategies available for assignment keys to nodes. For example, one can use hash, random, consistent hash, directory-based, round robin partition strategies. Every strategy has its own advantages and disadvantages. You can create you own partition strategy or you can implement an existing one. 
A partition strategy should satisfy the following 2 properties:

1. Every key belongs to a single node.
2. Keys are distributed (approximately) uniformly across the nodes.

We call a node an owner of a set of keys if this node is responsible for storing keys from the set. 

In this assignment, you need to develop a key-value store with a partition strategy that satisfies the above 2 conditions. Moreover, the key-value store should be resizable: we can add and remove nodes from the cluster while the key-value store is running. Therefore the partition strategy should be dynamic, that is, after the number of nodes have changed, the key-value store should rebalance keys between nodes.   
To test that keys are distributed evenly, you can insert a large amount of randomly generated keys and then count how many key each node owns. For example, if a key-value store consist of 3 nodes, you can add 3000 key to the store. Each node should contains approximately 1k of keys.

The key-value store should support the same functions put, get, del as in homework 2 with a difference: a response of every kvs command contains the ip:port pair of the owner node for that key. When a request (put, get or del) was issued to a node that is not supposed to have the key, the node forwards the request to the appropriate (owner) node and then returns the response PLUS the ip:port pair of the owner node.

Below is an example of responses of successful operations
put(key, val) returns json
```
    {
      'replaced': 0, // 1 if an existing key's val was replaced
      'msg': 'success'
      'owner': '10.0.0.20:8080'
    }
```
get(key) returns json
```
    {
      'msg' : 'success',
      'value': key,
      'owner': '10.0.0.20:8080'
    }
```

### Creating a Key-Value Store
We are going to use environmental variable "VIEW" to provide information about other nodes in the cluster. Variable "VIEW" contains ip:port pairs separated by a comma.
We are going to use environmental variable "IPPORT" to specify the ip address
and the port of the node we are starting. It might look redundant to provide the
ip address twice: the first time using the "--ip" flag and the second time
using the "IPPORT" environmental variable. However, we use the "--ip" flag due to Docker requirements to specify an ip address. We use the "IPPORT" variable for our convenience, as establishing the ip address within a running instance can be ambiguous.

Below is an example on how to start a key-value store that consists of 2 nodes:

```
docker run -p 8081:8080 --net=mynet --ip=10.0.0.20 -e VIEW="10.0.0.20:8080,10.0.0.21:8080" -e "IPPORT"="10.0.0.20:8080"  kvs
docker run -p 8082:8080 --net=mynet --ip=10.0.0.21 -e VIEW="10.0.0.20:8080,10.0.0.21:8080" -e "IPPORT"="10.0.0.21:8080"  kvs
```

All nodes listen to the 8080 port.

Your key-value store needs to be resizable. We can use environmental variables only when we start the nodes. However, once all nodes are running, we cannot use environmental variables to notify existing nodes about the cluster changes. Therefore your key-value store needs to support an API which notifies current nodes about a "view change" that could be a new member or loss of an old one. We assume the following API:

We add a node by sending a PUT request on "/kvs/view_update" with a parameter "type=add" and data field "ip_port" that contains a tuple ip:port of the new node. A successful request looks like this

* PUT localhost:8080/kvs/view_update?type=add  -d  "ip_port=10.0.0.20:8080"
```
{
     'msg': 'success'
}
```

Note that the IP:port pair of a new node is provided in the data field of the PUT request.
We remove a node by sending a PUT request on "/kvs/view_update" with parameter a "type=remove" and data field "ip_port" that contains a tuple ip:port of the node to be removed. An example request

* PUT localhost:8080/kvs/view_update?type=remove  -d  "ip_port=10.0.0.20:8080"
```
{
     'msg': 'success'
}
```

When a node receives a view change, it is responsible for notifying all of the other nodes of the view change and for moving its own keys to where they belong in the new view. 
Once the view change is successful on all nodes, the client receives the "success" response. We guarantee that we will wait until view changes are complete before sending any more traffic to a given node.

### Functional Guarantees
We are going to evaluate your key-value store using the following criteria
    
1. Operations "put", "get" and "del" should behave as expected on a single site key-value store  .
2. Every key should be present in one node only.
3. Keys should be distributed (approximately) evenly across the nodes. You can assume that keys are sampled uniformly at random. In other words, probability of observing any key is the same.
4. When a new node is added or removed, the keys should be re-distributed across the nodes so the above properties hold.
5. After a view change, the nodes should be able to accept put and get requests as before.
