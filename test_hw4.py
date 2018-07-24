from multiprocessing import Pool
import random
import string
from collections import Counter
import json
import os
import subprocess
import requests as req
import time

NODE_COUNTER = 2
PRINT_HTTP_REQUESTS = False
PRINT_HTTP_RESPONSES = False
AVAILABILITY_THRESHOLD = 1 
TB = 5

class Node:
    
    def __init__(self, access_port, ip, node_id):
        self.access_port = access_port
        self.ip = ip
        self.id = node_id

    def __repr__(self):
        return self.ip

def generate_random_keys(n):
    alphabet = string.ascii_lowercase
    keys = []
    for i in range(n):
        key = ''
        for _ in range(10):
            key += alphabet[random.randint(0, len(alphabet) - 1)]
        keys.append(key)
    return keys

def send_simple_get_request(hostname, node, key, causal_payload=''):
    """ The function does not check any conditions on the responce object.
    It returns raw request."""
    get_str = "http://" + hostname + ":" + node.access_port + "/kvs/" + key
    data = {'causal_payload':causal_payload}
    if PRINT_HTTP_REQUESTS:
        print "Get request: " + get_str + ' data field:' +  str(data)
    r = req.get(get_str, data=data)
    if PRINT_HTTP_RESPONSES:
        print r.text, r.status_code
    return r

def send_get_request(hostname, node, key, causal_payload=''):
    d = None
    get_str = "http://" + hostname + ":" + node.access_port + "/kvs/" + key
    data = {'causal_payload':causal_payload}
    try:
        if PRINT_HTTP_REQUESTS:
            print "Get request: " + get_str + ' data field:' +  str(data)
        start_time = time.time()
        r = req.get(get_str, data=data)
        end_time = time.time()
        if end_time - start_time > AVAILABILITY_THRESHOLD:
            print "THE SYSTEM IS NO AVAILABLE: GET request took too long to execute : %s seconds" % (end_time - start_time)
        if PRINT_HTTP_RESPONSES:
            print "Response:", r.text, r.status_code
        d = r.json()
        for field in ['msg', 'value', 'partition_id', 'causal_payload', 'timestamp']:
            if not d.has_key(field):
                raise Exception("Field \"" + field + "\" is not present in response " + str(d))
    except Exception as e:
        print "THE FOLLOWING GET REQUEST RESULTED IN AN ERROR: ",
        print get_str + ' data field ' +  str(data)
        print "Cannot retrieve key " + str(key) + " that should be present in the kvs"
        print e
    return d
    

def send_put_request(hostname, node, key, value, causal_payload=''):
    d = None
    put_str = "http://" + hostname + ":" + node.access_port + "/kvs/" + key
    data = {'val':value, 'causal_payload':causal_payload}
    try:
        if PRINT_HTTP_REQUESTS:
            print "PUT request:" + put_str + ' data field:' + str(data)
        start_time = time.time()
        r = req.put(put_str, data=data)
        end_time = time.time()
        if end_time - start_time > AVAILABILITY_THRESHOLD:
            print "THE SYSTEM IS NO AVAILABLE: PUT request took too long to execute : %s seconds" % (end_time - start_time)
        if PRINT_HTTP_RESPONSES:
            print "Response:", r.text, r.status_code
        d = r.json()
        for field in ['msg', 'partition_id', 'causal_payload', 'timestamp']:
            if not d.has_key(field):
                raise Exception("Field \"" + field + "\" is not present in response " + str(d))
    except Exception as e:
        print "THE FOLLOWING PUT REQUEST RESULTED IN AN ERROR: ",
        print put_str + ' data field ' +  str(data)
        print e
    return d

def send_put_request_randomized(hostname, nodes, keys, value=1, causal_payload=''):
    node = nodes[random.randint(0, len(nodes) - 1)]
    key = keys[random.randint(0, len(keys) - 1)]
    d = None
    return send_put_request(hostname, node, key, value=value, causal_payload=causal_payload)

def send_put_request_randomized_helper(arg):
    return send_put_request_randomized(*arg)

def add_keys(hostname, nodes, keys, value):
    d = {}
    for key in keys:
        resp_dict = send_put_request(hostname, nodes[random.randint(0, len(nodes) - 1)], key, value)
        partition_id = resp_dict['partition_id']
        if not d.has_key(partition_id):
            d[partition_id] = 0
        d[partition_id] += 1
    return d


def get_keys_distribution(hostname, nodes, keys):
    d = {}
    for key in keys:
        resp_dict = send_get_request(hostname, nodes[random.randint(0, len(nodes) - 1)], key)
        partition_id = resp_dict['partition_id']
        if not d.has_key(partition_id):
            d[partition_id] = 0
        d[partition_id] += 1
    return d

def generate_ip_port():
    global NODE_COUNTER
    NODE_COUNTER += 1
    ip = '10.0.0.' + str(NODE_COUNTER)
    port = str(8080 + NODE_COUNTER)
    return ip, port

def start_kvs(num_nodes, container_name, K=2, net='net', sudo='sudo'):
    ip_ports = []
    for i in range(1, num_nodes+1):
        ip, port = generate_ip_port()
        ip_ports.append((ip, port))
    view = ','.join([ip+":8080" for ip, _ in ip_ports])
    nodes = []
    print "Starting nodes"
    for ip, port in ip_ports:
        cmd_str = sudo + ' docker run -d -p ' + port + ":8080 --net=" + net + " -e K=" + str(K) + " --ip=" + ip + " -e VIEW=\"" + view + "\" -e IPPORT=\"" + ip + ":8080" + "\" " + container_name
        print cmd_str
        node_id = subprocess.check_output(cmd_str, shell=True).rstrip('\n')
        nodes.append(Node(port, ip, node_id))
    time.sleep(10)
    return nodes

def start_new_node(container_name, K=2, net='net', sudo='sudo'):
    ip, port = generate_ip_port()
    cmd_str = sudo + ' docker run -d -p ' + port + ":8080 --net=" + net + " -e K=" + str(K) + " --ip=" + ip + " -e IPPORT=\"" + ip + ":8080" + "\" " + container_name
    print cmd_str
    node_id = subprocess.check_output(cmd_str, shell=True).rstrip('\n')
    time.sleep(10)
    return Node(port, ip, node_id)

def stop_all_nodes(sudo):                                           
    running_containers = subprocess.check_output([sudo, 'docker',  'ps', '-q'])
    if len(running_containers):
        print "Stopping all nodes"
        os.system(sudo + " docker kill $(" + sudo + " docker ps -q)") 

def stop_node(node, sudo='sudo'):
    cmd_str = sudo + " docker kill %s" % node.id
    print cmd_str
    os.system(cmd_str)
    time.sleep(0.5)

def add_node_to_kvs(hostname, cur_node, new_node):
    d = None
    put_str = "http://" + hostname + ":" + cur_node.access_port + "/kvs/update_view?type=add"
    data = {'ip_port':new_node.ip + ":8080"}
    try:
        if PRINT_HTTP_REQUESTS:
            print "PUT request:" + put_str + " data field " + str(data)
        r = req.put(put_str, data=data)
        if PRINT_HTTP_RESPONSES:
            print "Response:", r.text, r.status_code
        d = r.json()
        if r.status_code not in [200, 201, '200', '201']:
            raise Exception("Error, status code %s is not 200 or 201" % r.status_code)
        for field in ['msg', 'partition_id', 'number_of_partitions']:
            if not d.has_key(field):
                raise Exception("Field \"" + field + "\" is not present in response " + str(d))
    except Exception as e:
        print "ERROR IN ADDING A NODE TO THE KEY-VALUE STORE:",
        print e
    return d


def delete_node_from_kvs(hostname, cur_node, node_to_delete):
    d = None
    put_str = "http://" + hostname + ":" + cur_node.access_port + "/kvs/update_view?type=remove"
    data = {'ip_port':node_to_delete.ip + ":8080"}
    try:
        if PRINT_HTTP_REQUESTS:
            print "PUT request: " + put_str + " data field " + str(data)
        r = req.put(put_str, data=data)
        if PRINT_HTTP_RESPONSES:
            print "Response:", r.text, r.status_code
        d = r.json()
        if r.status_code not in [200, 201, '200', '201']:
            raise Exception("Error, status code %s is not 200 or 201" % r.status_code)
        for field in ['msg', 'number_of_partitions']:
            if not d.has_key(field):
                raise Exception("Field \"" + field + "\" is not present in response " + str(d))
    except Exception as e:
        print "ERROR IN DELETING A NODE TO THE KEY-VALUE STORE:",
        print e
    return d

def get_partition_id_for_key(node, key):
    resp_dict = send_get_request(hostname, node, key, causal_payload='')
    return resp_dict['partition_id']

def get_partition_id_for_node(node):
    get_str = "http://" + hostname + ":" + node.access_port + "/kvs/get_partition_id"
    try:
        if PRINT_HTTP_REQUESTS:
            print "Get request: " + get_str
        r = req.get(get_str)
        if PRINT_HTTP_RESPONSES:
            print "Response:", r.text, r.status_code
        d = r.json()
        for field in ['msg', 'partition_id']:
            if not d.has_key(field):
                raise Exception("Field \"" + field + "\" is not present in response " + str(d))
    except Exception as e:
        print "THE FOLLOWING GET REQUEST RESULTED IN AN ERROR: ",
        print get_str + ' data field ' +  str(data)
        print e
    return d['partition_id']

def get_partition_members(node, partition_id):
    get_str = "http://" + hostname + ":" + node.access_port + "/kvs/get_partition_members"
    data = {'partition_id': partition_id}
    try:
        if PRINT_HTTP_REQUESTS:
            print "Get request: " + get_str + " data " + str(data)
        r = req.get(get_str, data=data)
        if PRINT_HTTP_RESPONSES:
            print "Response:", r.text, r.status_code
        d = r.json()
        for field in ['msg', 'partition_members']:
            if not d.has_key(field):
                raise Exception("Field \"" + field + "\" is not present in response " + str(d))
    except Exception as e:
        print "THE FOLLOWING GET REQUEST RESULTED IN AN ERROR: ",
        print get_str + ' data field ' +  str(data)
        print e
    return d['partition_members']

def get_all_partitions_ids(node):
    get_str = "http://" + hostname + ":" + node.access_port + "/kvs/get_all_partition_ids"
    try:
        if PRINT_HTTP_REQUESTS:
            print "Get request: " + get_str
        r = req.get(get_str)
        if PRINT_HTTP_RESPONSES:
            print "Response:", r.text, r.status_code
        d = r.json()
        for field in ['msg', 'partition_id_list']:
            if not d.has_key(field):
                raise Exception("Field \"" + field + "\" is not present in response " + str(d))
    except Exception as e:
        print "THE FOLLOWING GET REQUEST RESULTED IN AN ERROR: ",
        print get_str + ' data field ' +  str(data)
        print e
    return d['partition_id_list']

def find_node(nodes, ip_port):
    ip = ip_port.split(":")[0]
    for n in nodes:
        if n.ip == ip:
            return n
    return None

def disconnect_node(node, network, sudo):
    cmd_str = sudo + " docker network disconnect " + network + " " + node.id
    print cmd_str
    time.sleep(0.5)
    os.system(cmd_str)
    time.sleep(0.5)

def connect_node(node, network, sudo):
    cmd_str = sudo + " docker network connect " + network + " --ip=" + node.ip + ' ' + node.id
    print cmd_str
   # r = subprocess.check_output(cmd_str.split())
   # print r
    time.sleep(0.5)
    os.system(cmd_str)
    time.sleep(0.5)

if __name__ == "__main__":
    container_name = 'hw4'
    hostname = 'localhost'
    network = 'mynet'
    sudo = 'sudo'
    tests_to_run = [1, 2, 3, 4, 5, 6, 7, 8] #  
    if 1 in tests_to_run:
        try: # Test 1
            test_description = """ Test1:
            Node additions/deletions. A kvs consists of 2 partitions with 2 replicas each. 
            I add 3 new nodes. The number of partitions should become 4. Then I delete 2 nodes. 
            The number of partitions should become 3. """
            print test_description
            print 
            print "Starting kvs ..."
            nodes = start_kvs(4, container_name, K=2, net=network, sudo=sudo)

            print "Adding 3 nodes"
            n1 = start_new_node(container_name, K=2, net=network, sudo=sudo)
            n2 = start_new_node(container_name, K=2, net=network, sudo=sudo)
            n3 = start_new_node(container_name, K=2, net=network, sudo=sudo)

            resp_dict = add_node_to_kvs(hostname, nodes[0], n1)
            number_of_partitions = resp_dict.get('number_of_partitions')
            if number_of_partitions != 3:
                print "ERROR: the number of partitions should be 3, but it is " + str(number_of_partitions)
            else:
                print "OK, the number of partitions is 3"
            resp_dict = add_node_to_kvs(hostname, nodes[2], n2)
            number_of_partitions = resp_dict.get('number_of_partitions')
            if number_of_partitions != 3:
                print "ERROR: the number of partitions should be 3, but it is " + str(number_of_partitions)
            else:
                print "OK, the number of partitions is 3"
            resp_dict = add_node_to_kvs(hostname, n1, n3)
            number_of_partitions = resp_dict.get('number_of_partitions')
            if number_of_partitions != 4:
                print "ERROR: the number of partitions should be 4, but it is " + str(number_of_partitions)
            else:
                print "OK, the number of partitions is 4"
            print "Deleting nodes ..."
            resp_dict = delete_node_from_kvs(hostname, n3, nodes[0])
            number_of_partitions = resp_dict.get('number_of_partitions')
            if number_of_partitions != 3:
                print "ERROR: the number of partitions should be 3, but it is " + str(number_of_partitions)
            else:
                print "OK, the number of partitions is 3"
            resp_dict = delete_node_from_kvs(hostname, n3, nodes[2])
            number_of_partitions = resp_dict.get('number_of_partitions')
            if number_of_partitions != 3:
                print "ERROR: the number of partitions should be 3, but it is " + str(number_of_partitions)
            else:
                print "OK, the number of partitions is 3"
            resp_dict = delete_node_from_kvs(hostname, n3, n2)
            number_of_partitions = resp_dict.get('number_of_partitions')
            if number_of_partitions != 2:
                print "ERROR: the number of partitions should be 2, but it is " + str(number_of_partitions)
            else:
                print "OK, the number of partitions is 2"
            print "Stopping the kvs"
        except Exception as e:
            print "Exception in test 1"
            print e
        stop_all_nodes(sudo)
    if 2 in tests_to_run:
        try: # Test 2
            test_description = """ Test 2:
            A kvs consists of 2 partitions with 2 replicas each. I send 60 randomly generated keys to the kvs.
            I add 2 nodes to the kvs. No keys should be dropped.
            Then, I kill a node and send a view_change request to remove the faulty instance.
            Again, no keys should be dropped.
            """
            print test_description
            nodes = start_kvs(4, container_name, K=2, net=network, sudo=sudo)
            keys = generate_random_keys(60)
            add_keys(hostname, nodes, keys, 1)
            print "Adding 2 nodes"
            n1 = start_new_node(container_name, K=2, net=network, sudo=sudo)
            n2 = start_new_node(container_name, K=2, net=network, sudo=sudo)

            resp_dict1 = add_node_to_kvs(hostname, nodes[0], n1)
            time.sleep(2)
            resp_dict2 = add_node_to_kvs(hostname, nodes[2], n2)
            time.sleep(2)

            if not (resp_dict1 is not None and resp_dict2 is not None and 
                resp_dict1['msg'] == 'success' and resp_dict2['msg'] == 'success'):
                raise Exception("Problems with adding 2 new nodes")
            print "Nodes were successfully added. Verifying that no keys were dropped."

            distr = get_keys_distribution(hostname, nodes, keys)
            num_keys = sum([val for val in distr.itervalues()])
            if num_keys != len(keys):
                raise Exception("Some keys are missing after adding new nodes.")
            else:
                print "OK, no keys were dropped after adding new nodes."
            print "Stopping a node and sleeping for 5 seconds."
            stop_node(nodes[0], sudo=sudo)
            print "Sending a request to remove the faulty node from the key-value store."
            resp_dict = delete_node_from_kvs(hostname, n1, nodes[0])
            time.sleep(5)
            if not (resp_dict is not None and resp_dict['msg'] == 'success'):
                raise Exception("Problems with deleting a node ")
            print "A node was successfully deleted. Verifying that no keys were dropped."
            nodes[0] = n1
            nodes.append(n2)
            distr = get_keys_distribution(hostname, nodes, keys)
            num_keys = sum([val for val in distr.itervalues()])
            if num_keys != len(keys):
                raise Exception("Some keys are missing after deleting a node.")
            else:
                print "OK, no keys were dropped after deleting a node."
        except Exception as e:
            print "Exception in test 2"
            print e
        stop_all_nodes(sudo)
    if 3 in tests_to_run:
        try: # Test 3
            test_description = """ Test 3:
            Basic functionality for obtaining information about partitions; tests the following GET requests
            get_all_partitions_ids, get_partition_memebrs and get_partition_id.
            """
            print test_description
            nodes = start_kvs(4, container_name, K=2, net=network, sudo=sudo)
            keys = generate_random_keys(60)
            dist = add_keys(hostname, nodes, keys, 1)
            partition_id_list =  get_all_partitions_ids(nodes[0])
            if len(partition_id_list) != 2:
                raise Exception("ERROR: the number of partitions should be 2")
            for part_id in partition_id_list:
                if part_id not in dist:
                    raise Exception("ERROR: No keys are added to the partiton %s" % part_id)
            print "Obtaining partition id for key ", keys[0]
            partition_id_for_key = get_partition_id_for_key(nodes[0], keys[0])
            print "Obtaining partition members for partition ", partition_id_for_key
            members = get_partition_members(nodes[0], partition_id_for_key)
            if len(members) != 2:
                raise Exception("ERROR: the size of a partition %s should be 2, but it is %s" % (partition_id_for_key, len(members)))
            part_nodes = []
            for ip_port in members:
                n = find_node(nodes, ip_port)
                if n is None:
                    raise Exception("ERROR: mismatch in the node ids (likely bug in the test script)")
                part_nodes.append(n)
            print "Asking nodes directly about their partition id. Information should be consistent"
            for i in range(len(part_nodes)):
                part_id = get_partition_id_for_node(part_nodes[i])
                if part_id != partition_id_for_key:
                    raise Exception("ERRR: incosistent information about partition ids!")
            print "Ok, killing all the nodes in the partition ", partition_id_for_key
            print "Verifying that we cannot access the key using other partitions"
            for node in part_nodes:
                stop_node(node, sudo=sudo)
            other_nodes = [n for n in nodes if n not in part_nodes]

            get_str = "http://" + hostname + ":" + other_nodes[0].access_port + "/kvs/" + keys[0]
            data = {'causal_payload':''}
            if PRINT_HTTP_REQUESTS:
                print "Get request: " + get_str + ' data field:' +  str(data)
            r = req.get(get_str, data=data)
            if PRINT_HTTP_RESPONSES:
                print "Response:", r.text, r.status_code
            if r.status_code in [200, 201, '200', '201']:
                raise Exception("ERROR: A KEY %s SHOULD NOT BE AVAILABLE AS ITS PARTITION IS DOWN!!!" % keys[0])
            print "OK, functionality for obtaining information about partitions looks good!"
        except Exception as e:
            print "Exception in test 3"
            print e
        stop_all_nodes(sudo)
    if 4 in tests_to_run:
        try: # Test 4
            test_description = """ Test 4:
            A kvs consists of 2 partitions with 2 replicas each. I send 50 randomly generated keys to the kvs.
            I choose 2 keys from the same partition, then I send concurrently updates to these keys.
            No errors should occur. Then I sleep for few seconds, update each key and verify that the upades 
            are successful.
            """
            print test_description
            nodes = start_kvs(4, container_name, K=2, net=network, sudo=sudo)
            keys = generate_random_keys(50)
            add_keys(hostname, nodes, keys, 1)
            num_writes = 100
            num_keys = 5
            num_procs = 10
            pool = Pool(processes=num_procs)
            print "%s processes performs %s writes concurrently on %s keys" % (num_procs, num_writes, num_keys)
            time.sleep(5)
            args = [[hostname, nodes, keys[0:num_keys], v, ''] for v in range(num_writes)]
            result = pool.map(send_put_request_randomized_helper, args)
            pool.close()
            pool.join()
            if PRINT_HTTP_RESPONSES:
                print result
            time.sleep(1)
            d = send_put_request(hostname, nodes[0], keys[0], 11, causal_payload='')
            d = send_get_request(hostname, nodes[2], keys[0], causal_payload=d['causal_payload'])
            if int(d['value']) == 11:
                print "OK, the key-value store works after spamming"
            else:
                raise Exception("ERROR: the key-value store did not process PUT/GET requests properly after spamming")
        except Exception as e:
            print "Exception in test 4"
            print e
        stop_all_nodes(sudo)
    if 5 in tests_to_run:
        try: # Test 5
            test_description = """ Test 5:
            A very simple test to verify that after we disconnect/connect a node, the kvs works as it is supposed to.
            The kvs consists of one node only.
            """
            print test_description
            nodes = start_kvs(1, container_name, K=1, net=network, sudo=sudo)
            node = nodes[0]
            d = send_put_request(hostname, node, 'foo', 'zoo', causal_payload='')
            d = send_get_request(hostname, node, 'foo', causal_payload=d['causal_payload'])
            if d['value'] != 'zoo':
                raise Exception("ERROR: the kvs did not store value zoo for key zoo")
            disconnect_node(node, network, sudo)
            time.sleep(1)
            connect_node(node, network, sudo)
            time.sleep(TB)
            d = send_get_request(hostname, node, 'foo', causal_payload=d['causal_payload'])
            if not d.has_key('value')  or d['value'] != 'zoo':
                raise Exception("ERROR: the kvs is not working after network healed.")
            print "OK, the kvs works after we disconnected the node and connected it back."
        except Exception as e:
            print "Exception in test 5"
            print e
        stop_all_nodes(sudo)
    if 6 in tests_to_run:
        try: # Test 6
            num_keys = 3
            test_description = """ Test 6:
            A test with a network partition.
            A kvs consists of 2 partitions with 2 replicas each. I send %s randomly generated key(s) to the kvs.
            I disconnect a node and update a key from that partition, then I connect the node back, wait for %s
            seconds and disconnect the other node in partition.
            I verify that the key is updated.
            """ % (num_keys, TB)
            print test_description
            nodes = start_kvs(4, container_name, K=2, net=network, sudo=sudo)
            keys = generate_random_keys(num_keys)
            add_keys(hostname, nodes, keys, -1)
            #d = send_put_request(hostname, nodes[0], keys[0], -1)
            partition_id = get_partition_id_for_key(nodes[0], keys[0])
            members = get_partition_members(nodes[0], partition_id)
            part_nodes = [find_node(nodes, ip_port) for ip_port in members]
            print "key %s belongs to partition %s with nodes %s and %s" % (keys[0], partition_id, part_nodes[0], part_nodes[1])
            print "Disconnecting both nodes to verify that the key is not available"
            disconnect_node(part_nodes[0], network, sudo)
            disconnect_node(part_nodes[1], network, sudo)
            other_nodes = [n for n in nodes if n not in part_nodes]
            r = send_simple_get_request(hostname, other_nodes[0], keys[0], causal_payload='')
            if r.status_code in [200, 201, '200', '201']:
                raise Exception("ERROR: A KEY %s SHOULD NOT BE AVAILABLE AS ITS PARTITION IS DOWN!!!" % keys[0])
            print "Good, the key is not available"
            print "Connecting one node back and verifying that the key is accessible"
            connect_node(part_nodes[1], network, sudo)
            time.sleep(TB)
            r = send_simple_get_request(hostname, other_nodes[0], keys[0], causal_payload='')
            d = r.json()
            print d
            if not d.has_key('value') or int(d['value']) != -1:
                raise Exception("ERROR: service is not availabe or the value of the key changed after the network healed")
            print "Good, the key is available"
            print "Update the key"
            d = send_put_request(hostname, other_nodes[0],  keys[0], 17, causal_payload=d['causal_payload'])
            connect_node(part_nodes[0], network, sudo)
            time.sleep(TB)
            disconnect_node(part_nodes[1], network, sudo)
            time.sleep(1)
            d = send_get_request(hostname, other_nodes[1], keys[0], causal_payload=d['causal_payload'])
            if int(d['value']) != 17:
                raise Exception("ERROR: THE VALUE IS STALE AFTER NETWORK HEALED AND %s SECONDS!" % TB)
            print "OK, the value is up to date!!!"


        except Exception as e:
            print "Exception in test 6"
            print e
        stop_all_nodes(sudo)
    if 7 in tests_to_run:
        try: # Test 7
            num_keys = 3
            test_description = """ Test 7:
            A kvs consists of 2 partitions with 3 replicas each. I send %s randomly generated key(s) to the kvs.
            I read a key from a node. The I update the key on another node from the same partition, 
            providing the causal payload from the first read. Then I write a new value to the key on another node. 
            I read from the 3rd node in the paritition and verify that the value is fresh.
            """ % (num_keys)
            print test_description
            nodes = start_kvs(6, container_name, K=3, net=network, sudo=sudo)
            keys = generate_random_keys(num_keys)
            add_keys(hostname, nodes, keys, -1)
            #d = send_put_request(hostname, nodes[0], keys[0], -1)
            partition_id = get_partition_id_for_key(nodes[0], keys[0])
            members = get_partition_members(nodes[0], partition_id)
            part_nodes = [find_node(nodes, ip_port) for ip_port in members]
            print "key %s belongs to partition %s with nodes %s and %s" % (keys[0], partition_id, part_nodes[0], part_nodes[1])
            r = send_simple_get_request(hostname, part_nodes[0], keys[0], causal_payload='')
            d = r.json()
            d = send_put_request(hostname, part_nodes[1],  keys[0], 15, causal_payload=d['causal_payload'])
            r = send_simple_get_request(hostname, part_nodes[2],  keys[0], causal_payload=d['causal_payload'])
            d = r.json()
            if int(d['value']) != 15:
                raise Exception("ERROR: THE VALUE IS STALE !")
            print "OK, the value is up to date!!!"
        except Exception as e:
            print "Exception in test 7"
            print e
        stop_all_nodes(sudo)
    if 8 in tests_to_run:
        try: # Test 8
            num_keys = 100
            test_description = """ Test 8:
            A kvs consists of 2 partitions with 2 replicas each. 
            I add %s randomly generate keys to the kvs. I remove 1 partition (2 nodes) and 
            check whether any keys were dropped. (This test is different from other ones as they did not
            test whether keys were droppend after number of partitions decremented.)
            """ % (num_keys)
            print test_description
            nodes = start_kvs(4, container_name, K=2, net=network, sudo=sudo)
            keys = generate_random_keys(num_keys)
            add_keys(hostname, nodes, keys, -1)
            #d = send_put_request(hostname, nodes[0], keys[0], -1)
            partition_id = get_partition_id_for_key(nodes[0], keys[0])
            members = get_partition_members(nodes[0], partition_id)
            part_nodes = [find_node(nodes, ip_port) for ip_port in members]
            other_nodes = [n for n in nodes if n not in part_nodes]
            resp_dict = delete_node_from_kvs(hostname, other_nodes[0], part_nodes[0])
            time.sleep(10)
            resp_dict = delete_node_from_kvs(hostname, other_nodes[0], part_nodes[1])
            time.sleep(10)
            distr = get_keys_distribution(hostname, other_nodes, keys)
            num_keys = sum([val for val in distr.itervalues()])
            if num_keys != len(keys):
                raise Exception("Some keys are missing after removing a partition.")
            else:
                print "OK, no keys were dropped after removing a partition."
        except Exception as e:
            print "Exception in test 8"
            print e
        stop_all_nodes(sudo)
