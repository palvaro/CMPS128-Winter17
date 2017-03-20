import random
import string
from collections import Counter
import json
import os
import subprocess
import requests as req
import time
import numpy as np

NODE_COUNTER = 2
PRINT_HTTP_REQUESTS = True
PRINT_HTTP_RESPONSES = False

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

def send_get_request(hostname, node, key):
    owner, value = None, None
    try:
        if PRINT_HTTP_REQUESTS:
            print "GET request: http://" + hostname + ":" + node.access_port + "/kvs/" + key
        r = req.get("http://" + hostname + ":" + node.access_port + "/kvs/" + key)
        if PRINT_HTTP_RESPONSES:
            print "Response:", r.text, r.status_code
        d = r.json()
       # print "http://" + hostname + ":" + node.access_port + "/kvs/" + key
       # print d
        owner = d['owner']
        value = d['value']
    except Exception as e:
        print "THE FOLLOWING GET REQUEST RESULTED IN AN ERROR: http://" + hostname + ":" + node.access_port + "/kvs/" + key
        print "Cannot retrieve key " + str(key) + " that should be present in the kvs"
       # exit(1)
    return owner, value
    
def send_put_request(hostname, node, key, value):
    #print "sending put request " + "http://" + hostname + ":" + node.access_port + "/kvs/" + key
    if PRINT_HTTP_REQUESTS:
        print "PUT request:" + "http://" + hostname + ":" + node.access_port + "/kvs/" + key + "; value " + str(value)
    r = req.put("http://" + hostname + ":" + node.access_port + "/kvs/" + key, data={'val':value})
    if PRINT_HTTP_RESPONSES:
        print "Response:", r.text, r.status_code
    d = r.json()
    #print r.text
    return d['owner'], d['msg']

def add_keys(hostname, nodes, keys, value):
    d = {}
    for n in nodes:
        d[n.ip + ":8080"] = 0
    for key in keys:
        owner, msg = send_put_request(hostname, nodes[random.randint(0, len(nodes) - 1)], key, value)
        #print "Added key " + key  + " to node " + owner 
        d[owner] += 1
    return d

def get_keys_distribution(hostname, nodes, keys):
    d = {}
    for n in nodes:
        d[n.ip + ":8080"] = 0
    for key in keys:
        owner, val = send_get_request(hostname, nodes[random.randint(0, len(nodes) - 1)], key)
        if owner is not None:
            d[owner] += 1
    return d

def add_node_to_kvs(hostname, cur_node, new_node):
    if PRINT_HTTP_REQUESTS:
        print "PUT request: " + "http://" + hostname + ":" + cur_node.access_port + "/kvs/view_update?type=add" + "; data field is " + 'ip_port' + new_node.ip + ":8080"
    r = req.put("http://" + hostname + ":" + cur_node.access_port + "/kvs/view_update?type=add", 
            data={'ip_port':new_node.ip + ":8080"})
    if PRINT_HTTP_RESPONSES:
        print "Response:", r.text, r.status_code
    return r.status_code, r.json()['msg']

def delete_node_from_kvs(hostname, cur_node, node_to_delete):
    if PRINT_HTTP_REQUESTS:
        print "PUT request: " + "http://" + hostname + ":" + cur_node.access_port + "/kvs/view_update?type=remove" + "; data field is " + 'ip_port=' + node_to_delete.ip + ":8080"
    r = req.put("http://" + hostname + ":" + cur_node.access_port + "/kvs/view_update?type=remove",
            data={'ip_port':node_to_delete.ip + ":8080"})
    if PRINT_HTTP_RESPONSES:
        print "Response:", r.text, r.status_code
    return r.status_code, r.json()['msg']

def generate_ip_port():
    global NODE_COUNTER
    NODE_COUNTER += 1
    ip = '10.0.0.' + str(NODE_COUNTER)
    port = str(8080 + NODE_COUNTER)
    return ip, port

def start_kvs(num_nodes, container_name, net='net', sudo='sudo', sleep_time=10):
    ip_ports = []
    for i in range(1, num_nodes+1):
        ip, port = generate_ip_port()
        ip_ports.append((ip, port))
    view = ','.join([ip+":8080" for ip, _ in ip_ports])
    nodes = []
    print "Starting nodes"
    for ip, port in ip_ports:
        cmd_str = sudo + ' docker run -d -p ' + port + ":8080 --net=" + net + " --ip=" + ip + " -e VIEW=\"" + view + "\" -e IPPORT=\"" + ip + ":8080" + "\" " + container_name
        print cmd_str
        node_id = subprocess.check_output(cmd_str, shell=True).rstrip('\n')
        nodes.append(Node(port, ip, node_id))
    time.sleep(sleep_time)
    return nodes

def start_new_node(container_name, net='net', sudo='sudo', sleep_time=5):
    ip, port = generate_ip_port()
    cmd_str = sudo + ' docker run -d -p ' + port + ":8080 --net=" + net + " --ip=" + ip + " -e IPPORT=\"" + ip + ":8080" + "\" " + container_name
    print cmd_str
    node_id = subprocess.check_output(cmd_str, shell=True).rstrip('\n')
    time.sleep(sleep_time)
    return Node(port, ip, node_id)

def stop_all_nodes(sudo):                                           
    print "Stopping all nodes"
    os.system(sudo + " docker kill $(" + sudo + " docker ps -q)") 

def stop_node(node, sudo='sudo'):
    cmd_str = sudo + " docker kill %s" % node.id
    print cmd_str
    os.system(cmd_str)

def are_counts_balanced(counts, threshold):
    is_balanced = True
    for node, count in counts.iteritems():
        if count < threshold:
            is_balanced = False
    return is_balanced

def is_balanced_on_avg(counts, threshold=0.9):
    mean = np.average(counts)
    std = np.std(counts)
    return std / mean < threshold
    

if __name__ == "__main__":
    # Name of you docker image
    num_rounds = 5
    # If you can run docker without using sudo, then make the sudo variable empty string
    sudo = 'sudo'
    # For windows users hostname should be set to the virtual box ip address
    hostname = 'localhost'
    # Number of random keys to generate
    num_keys = 100

    # List of containers to test
    container_list = [ ]

    for container_name in container_list:
        print "\n\n"
        print "Testing container", container_name
        try:
            counts_beginning = []
            counts_addition = []
            counts_deletion = []
            for i in range(num_rounds):
                print "ROUND", i
                num_nodes = 3
                print "Starting a KVS with " + str(num_nodes) + " nodes"
                kvs_nodes = start_kvs(num_nodes, container_name, net='mynet', sudo=sudo)
                #print kvs_nodes
                keys = generate_random_keys(num_keys)
                print "Adding " + str(num_keys) + " randomly generated keys "
                counts = add_keys(hostname, kvs_nodes, keys, value = 1)
                if sum([val for _, val in counts.iteritems()]) != num_keys:
                    print "SOME KEYS WERE NOT ADDDED SUCCESSFULY"
                counts = get_keys_distribution(hostname, kvs_nodes, keys)
                for k, v in counts.iteritems():
                    counts_beginning.append(v)
                print "Adding a new node"
                new_node = start_new_node(container_name, net='mynet', sudo=sudo)
                kvs_nodes.append(new_node)
                status_code, msg = add_node_to_kvs(hostname, kvs_nodes[0], new_node)
                time.sleep(10)

                counts = get_keys_distribution(hostname, kvs_nodes, keys)
                for k, v in counts.iteritems():
                    counts_addition.append(v)
                if sum([val for _, val in counts.iteritems()]) != num_keys:
                    print "SOME KEYS WERE LOST AFTER AN ADDITION OF A NODE"
                print "\nDeleting a node"
                delete_node_from_kvs(hostname, kvs_nodes[0], kvs_nodes[1])
                time.sleep(10)
                stop_node(kvs_nodes[1], sudo=sudo)
                del kvs_nodes[1]
                counts = get_keys_distribution(hostname, kvs_nodes, keys)
                for k, v in counts.iteritems():
                    counts_deletion.append(v)
                if sum([val for _, val in counts.iteritems()]) != num_keys:
                    print "SOME KEYS WERE LOST AFTER DELETION OF A NODE"
                stop_all_nodes(sudo)
            print "average, stdev, min, max"
            print "Beginning", np.average(counts_beginning), np.std(counts_beginning), min(counts_beginning), max(counts_beginning)
            if is_balanced_on_avg(counts_beginning):
                print "OK, balanced"
            else:
                print "NOT BALANCED"
            print "Addition", np.average(counts_addition), np.std(counts_addition), min(counts_addition), max(counts_addition)
            if is_balanced_on_avg(counts_addition):
                print "OK, balanced"
            else:
                print "NOT BALANCED"
            print "Deletion", np.average(counts_deletion), np.std(counts_deletion), min(counts_deletion), max(counts_deletion)
            if is_balanced_on_avg(counts_deletion):
                print "OK, balanced"
            else:
                print "NOT BALANCED"
        except Exception as e:
            print "Exception while testing container ", container_name
            print e
            stop_all_nodes(sudo)
