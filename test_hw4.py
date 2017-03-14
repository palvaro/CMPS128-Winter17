
import random
import string
from collections import Counter
import json
import os
import subprocess
import requests as req
import time

NODE_COUNTER = 2
PRINT_HTTP_REQUESTS = True
PRINT_HTTP_RESPONSES = True

class Node:
    
    def __init__(self, access_port, ip, node_id):
        self.access_port = access_port
        self.ip = ip
        self.id = node_id

    def __repr__(self):
        s = self.ip + " " + self.access_port + " " + self.id[:7]
        return s

def generate_random_keys(n):
    alphabet = string.ascii_lowercase
    keys = []
    for i in range(n):
        key = ''
        for _ in range(10):
            key += alphabet[random.randint(0, len(alphabet) - 1)]
        keys.append(key)
    return keys


def send_get_request(hostname, node, key, causal_payload=''):
    d = None
    get_str = "http://" + hostname + ":" + node.access_port + "/kvs/" + key
    data = {'causal_payload':causal_payload}
    try:
        if PRINT_HTTP_REQUESTS:
            print "Get request: " + get_str + ' data field:' +  str(data)
        r = req.get(get_str, data=data)
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
        r = req.put(put_str, data=data)
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
    print "Stopping all nodes"
    os.system(sudo + " docker kill $(" + sudo + " docker ps -q)") 

def stop_node(node, sudo='sudo'):
    cmd_str = sudo + " docker kill %s" % node.id
    print cmd_str
    os.system(cmd_str)

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


if __name__ == "__main__":
    container_name = 'jpitz_hw4'
    hostname = 'localhost'
    network = 'mynet'
    sudo = 'sudo'

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
            print "ERROR: number of partitions should be 3, but it is " + str(number_of_partitions)
        else:
            print "OK, number of partitions is 3"
        resp_dict = add_node_to_kvs(hostname, nodes[2], n2)
        number_of_partitions = resp_dict.get('number_of_partitions')
        if number_of_partitions != 3:
            print "ERROR: number of partitions should be 3, but it is " + str(number_of_partitions)
        else:
            print "OK, number of partitions is 3"
        resp_dict = add_node_to_kvs(hostname, n1, n3)
        number_of_partitions = resp_dict.get('number_of_partitions')
        if number_of_partitions != 4:
            print "ERROR: number of partitions should be 4, but it is " + str(number_of_partitions)
        else:
            print "OK, number of partitions is 4"
        print "Deleting nodes ..."
        resp_dict = delete_node_from_kvs(hostname, n3, nodes[0])
        number_of_partitions = resp_dict.get('number_of_partitions')
        if number_of_partitions != 3:
            print "ERROR: number of partitions should be 3, but it is " + str(number_of_partitions)
        else:
            print "OK, number of partitions is 3"
        resp_dict = delete_node_from_kvs(hostname, n3, nodes[2])
        number_of_partitions = resp_dict.get('number_of_partitions')
        if number_of_partitions != 3:
            print "ERROR: number of partitions should be 3, but it is " + str(number_of_partitions)
        else:
            print "OK, number of partitions is 3"
        resp_dict = delete_node_from_kvs(hostname, n3, n2)
        number_of_partitions = resp_dict.get('number_of_partitions')
        if number_of_partitions != 2:
            print "ERROR: number of partitions should be 2, but it is " + str(number_of_partitions)
        else:
            print "OK, number of partitions is 2"
        print "Stopping the kvs"
        stop_all_nodes(sudo)
    except Exception as e:
        print "Exception in test 1"
        print e
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
        resp_dict2 = add_node_to_kvs(hostname, nodes[2], n2)

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
        time.sleep(5)
        print "Sending a request to remove faulty node from the key-value store."
        resp_dict = delete_node_from_kvs(hostname, n1, nodes[0])
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
