import sys, os, re, socket

from threading import Thread, Lock
import time
from random import shuffle
from pathlib import Path

from utils.hashtable import HashTable
from utils.consistent_hashing import ConsistentHashing
from utils.commit_log import CommitLog
from utils import utils


class HashTableService:
    def __init__(self, ip, port, partitions):
        self.ip = ip
        self.port = port
        self.ht = HashTable()

        if not os.path.exists("commit-logs"):
            os.mkdir("commit-logs")

        self.commit_log = CommitLog(file=f"commit-logs/commit-log-{self.ip}-{self.port}.txt")
        self.chash = ConsistentHashing()
        self.partitions = eval(partitions)
        self.conns = [[None] * len(self.partitions[i]) for i in range(len(self.partitions))]
        self.is_leader = False
        self.cluster_index = -1
        self.cluster_lock = Lock()
        self.socket_locks = [[Lock() for j in range(len(self.partitions[i]))] for i in range(len(self.partitions))]
        self.commit_temp = {}
        self.commit_temp_lock = Lock()

        self.initialize_commit_log()

        for i in range(len(self.partitions)):
            cluster = self.partitions[i]

            for j in range(len(cluster)):
                ip, port = cluster[j].split(':')
                port = int(port)

                if (ip, port) == (self.ip, self.port):
                    self.cluster_index = i
                    # The first ip:port in each array is assigned to be leader
                    if j == 0:
                        self.is_leader = True
                else:
                    # 3rd element is the socket object
                    self.conns[i][j] = [ip, port, None]

        print(self.conns)
        self.consistent_hash_join()

        utils.run_thread(fn=self.join_replica, args=())
        utils.run_thread(fn=self.join_cluster, args=())

        print("Ready....")

    def initialize_commit_log(self):
        # Initialize commit log file
        commit_logfile = Path(self.commit_log.file)
        commit_logfile.touch(exist_ok=True)
        with open(self.commit_log.file, 'r+') as f:
            f.truncate(0)

    def consistent_hash_join(self):
        # Add leader nodes to consistent hashing
        for i in range(len(self.partitions)):
            added = self.chash.add_node_hash(str(i))
            assert added == 1

    def join_replica(self):
        # Replica asks leaders to add itself
        if self.is_leader is False:
            # Send message to all leaders because during 'get' some leader other than own leader might need to forward request to this replica
            msg = f"join {self.ip} {self.port} {self.cluster_index}"
            resp = utils.broadcast_join(msg, self.conns, self.cluster_lock, self.socket_locks)
            assert resp == True

            # Get commitlog from own leader and update own ht
            # Wait for own leader to be ready
            while True:
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    sock.connect((str(self.conns[self.cluster_index][0][0]),
                                  int(self.conns[self.cluster_index][0][1])))

                    self.commit_log.write_log_from_sock(sock)
                    sock.close()
                    break
                except Exception as e:
                    print(e)
                    time.sleep(0.5)

            # Get commands in-memory from log file and insert into own ht
            commands = self.commit_log.read_log()

            for cmd in commands:
                parts = cmd.split(" ")
                if len(parts) == 4:
                    # set operation
                    op, key, value, req_id = parts
                else:
                    # delete operations
                    op, key, req_id = parts

                req_id = int(req_id)

                if op == 'set':
                    self.ht.set(key=key, value=value, req_id=req_id)
                else:
                    self.ht.delete(key=key, req_id=req_id)

    def join_cluster(self):
        # Leader asks other leaders to add itself
        if self.is_leader:
            print("I am leader")
            # Send message to all leaders other than itself
            msg = f"join {self.ip} {self.port} {self.cluster_index}"
            resp = utils.broadcast_join(msg, self.conns, self.cluster_lock, self.socket_locks, self.cluster_index)
            assert resp == True

            # Get all next nodes in consistent hash ring. Some keys that were mapped to these nodes
            # will now be mapped to this new leader node.
            nodes = self.chash.get_next_nodes_from_node(str(self.cluster_index))

            for next_node in nodes:
                next_node = int(next_node)

                self.commit_log_temp = CommitLog(f"commit-log-temp-{self.ip}-{self.port}.txt")

                commit_logfile = Path(self.commit_log_temp.file)
                commit_logfile.touch(exist_ok=True)

                # Get commitlog from leader and update own ht
                while True:
                    try:
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                        sock.connect((str(self.conns[next_node][0][0]),
                                      int(self.conns[next_node][0][1])))

                        self.commit_log_temp.write_log_from_sock(sock)
                        sock.close()
                        break
                    except Exception as e:
                        print(e)
                        time.sleep(0.5)

                # Update commit log with only those keys for which are before current node
                # in the consistent hash ring and not all keys.
                # Create temp log file for these.
                commands = self.commit_log_temp.read_log()

                for cmd in commands:
                    parts = cmd.split(" ")
                    if len(parts) == 4:
                        # set operation
                        op, key, value, req_id = parts
                    else:
                        # delete operations
                        op, key, req_id = parts

                    nxt = int(self.chash.get_next_node(key))

                    if nxt == self.cluster_index:
                        req_id = int(req_id)
                        if op == 'set':
                            ret = self.ht.set(key=key, value=value, req_id=req_id)
                        else:
                            ret = self.ht.delete(key=key, req_id=req_id)

                        if ret == 1:
                            self.commit_log.log(cmd)

                        # send delete message for moved keys
                        msg = f"del-no-fwd {key} {req_id}"
                        resp = utils.send_and_recv(msg, self.conns[next_node], self.socket_locks[next_node], 0)
                        assert resp == "ok"

                os.remove(self.commit_log_temp.file)

    def handle_commands(self, msg, conn):
        set_ht = re.match('^set ([a-zA-Z0-9]+) ([a-zA-Z0-9]+) ([0-9]+)$', msg)
        get_ht = re.match('^get ([a-zA-Z0-9]+) ([0-9]+)$', msg)
        del_ht = re.match('^del ([a-zA-Z0-9]+) ([0-9]+)$', msg)
        del_ht_no_fwd = re.match('^del-no-fwd ([a-zA-Z0-9]+) ([0-9]+)$', msg)
        replica_join = re.match('^join ([0-9\.]+) ([0-9]+) ([0-9]+)$', msg)
        committxn = re.match('^committxn ([a-zA-Z\-]+) ([a-zA-Z0-9]+) ([0-9]+)$', msg)
        log = re.match('^commitlog$', msg)
        topology = re.match('^topology$', msg)

        if set_ht:
            output = "ko"
            try:
                key, value, req_id = set_ht.groups()
                req_id = int(req_id)
                node = int(self.chash.get_next_node(key))

                if self.cluster_index == node:
                    # The key is intended for current cluster

                    # Prevent reading key while it is being updated and replicated
                    with self.commit_temp_lock:
                        if key not in self.commit_temp:
                            self.commit_temp[key] = {}

                        # Same key might come in for multiple req_id's
                        self.commit_temp[key][req_id] = value

                    if self.is_leader:
                        # Replicate if this is leader server and req_id is the latest one corresponding to key
                        replicated = utils.broadcast_write(msg, self.conns[node], self.cluster_lock,
                                                           self.socket_locks[node])

                        if replicated:
                            commited = utils.broadcast_write(f"committxn set {key} {req_id}", self.conns[node],
                                                             self.cluster_lock, self.socket_locks[node])

                            if commited:
                                ret = self.ht.set(key=key, value=value, req_id=req_id)
                                if ret == 1:
                                    self.commit_log.log(msg)

                                # req_id commit is completed
                                self.commit_temp[key].pop(req_id)
                                output = "ok"
                    else:
                        output = "ok"
                else:
                    # Forward to relevant cluster if key is not intended for this cluster
                    output = utils.send_and_recv(msg, self.conns[node], self.socket_locks[node], 0)
                    if output is None:
                        output = "ko"

            except Exception as e:
                print(e)

        elif get_ht:
            output = "ko"

            try:
                key, _ = get_ht.groups()
                node = int(self.chash.get_next_node(key))

                if self.cluster_index == node:
                    # The key is intended for current cluster

                    while True:
                        # Key is being updated and replicated
                        if key not in self.commit_temp or len(self.commit_temp[key]) == 0:
                            break

                        # retry if update is not yet commited
                        time.sleep(0.5)

                    output = self.ht.get_value(key=key)
                    if output:
                        output = str(output)

                else:
                    # Forward the get request to a random node in the correct cluster
                    with self.cluster_lock:
                        indices = list(range(len(self.partitions[node])))

                    shuffle(indices)
                    # Loop over multiple indices because some replica might be unresponsive and times out
                    for j in indices:
                        output = utils.send_and_recv(msg, self.conns[node], self.socket_locks[node], j, timeout=10)
                        if output:
                            break

                if output is None:
                    output = 'Error: Non existent key'

            except Exception as e:
                print(e)

        elif del_ht:
            output = "ko"

            try:
                key, req_id = del_ht.groups()
                req_id = int(req_id)
                node = int(self.chash.get_next_node(key))

                if self.cluster_index == node:
                    # The key is intended for current cluster

                    # Prevent reading key while it is being updated and replicated
                    with self.commit_temp_lock:
                        if key not in self.commit_temp:
                            self.commit_temp[key] = {}

                        # Same key might come in for multiple req_id's
                        self.commit_temp[key][req_id] = None

                    if self.is_leader:
                        # Replicate if this is leader server and req_id is the latest one corresponding to key
                        replicated = utils.broadcast_write(msg, self.conns[node], self.cluster_lock,
                                                           self.socket_locks[node])

                        if replicated:
                            commited = utils.broadcast_write(f"committxn del {key} {req_id}", self.conns[node],
                                                             self.cluster_lock, self.socket_locks[node])
                            if commited:
                                ret = self.ht.delete(key=key, req_id=req_id)
                                if ret == 1:
                                    self.commit_log.log(msg)

                                # req_id commit is completed
                                self.commit_temp[key].pop(req_id)
                                output = "ok"
                    else:
                        output = "ok"
                else:
                    # Forward to relevant cluster if key is not intended for this cluster
                    output = utils.send_and_recv(msg, self.conns[node], self.socket_locks[node], 0)
                    if output is None:
                        output = "ko"

            except Exception as e:
                print(e)

        elif del_ht_no_fwd:
            output = "ko"

            try:
                key, req_id = del_ht_no_fwd.groups()
                req_id = int(req_id)

                # Prevent reading key while it is being updated and replicated
                with self.commit_temp_lock:
                    if key not in self.commit_temp:
                        self.commit_temp[key] = {}

                    # Same key might come in for multiple req_id's
                    self.commit_temp[key][req_id] = None

                # The key is intended for current cluster
                if self.is_leader:
                    # Replicate if this is leader server and req_id is the latest one corresponding to key
                    replicated = utils.broadcast_write(msg, self.conns[self.cluster_index], self.cluster_lock,
                                                       self.socket_locks[self.cluster_index])

                    if replicated:
                        commited = utils.broadcast_write(f"committxn del-no-fwd {key} {req_id}",
                                                         self.conns[self.cluster_index], self.cluster_lock,
                                                         self.socket_locks[self.cluster_index])
                        if commited:
                            ret = self.ht.delete(key=key, req_id=req_id)
                            if ret == 1:
                                self.commit_log.log(msg)

                            # req_id commit is completed
                            self.commit_temp[key].pop(req_id)
                            output = "ok"
                else:
                    output = "ok"

            except Exception as e:
                print(e)

        elif replica_join:
            # Add new replica if not already added
            output = "ko"

            try:
                ip, port, index = replica_join.groups()
                ip_str = f"{ip}:{port}"
                index = int(index)

                with self.cluster_lock:
                    # Add new cluster
                    if index >= len(self.partitions):
                        port = int(port)
                        self.partitions.append([ip_str])
                        self.conns.append([[ip, port, None]])
                        self.socket_locks.append([Lock()])
                        self.chash.add_node_hash(str(index))
                        output = "ok"

                    # Add new replica if it is leader and not already added
                    elif self.is_leader and ip_str not in self.partitions[index]:
                        port = int(port)
                        self.partitions[index].append(ip_str)
                        self.conns[index].append([ip, port, None])
                        self.socket_locks[index].append(Lock())
                        output = "ok"

                    else:
                        output = "ok"

            except Exception as e:
                print(e)

        elif log:
            output = "ko"

            try:
                # Send commit log file
                self.commit_log.send_log_to_sock(conn)
                output = ""
                conn.close()
            except Exception as e:
                print(e)

        elif topology:
            output = "ko"
            try:
                # Send topology
                self.send_topology_to_sock(conn)
                output = ""
                conn.close()
            except Exception as e:
                print(e)

        elif committxn:
            output = "ko"

            try:
                op, key, req_id = committxn.groups()
                req_id = int(req_id)

                val = self.commit_temp[key][req_id]

                if op == "set":
                    ret = self.ht.set(key=key, value=val, req_id=req_id)
                    if ret == 1:
                        self.commit_log.log(f"set {key} {val} {req_id}")

                elif op == "del":
                    ret = self.ht.delete(key=key, req_id=req_id)
                    if ret == 1:
                        self.commit_log.log(f"del {key} {req_id}")

                elif op == "del-no-fwd":
                    ret = self.ht.delete(key=key, req_id=req_id)
                    if ret == 1:
                        self.commit_log.log(f"del-no-fwd {key} {req_id}")
                else:
                    raise Exception("Invalid operator")

                self.commit_temp[key].pop(req_id)
                output = "ok"

            except Exception as e:
                print(e)
                
        elif msg == "heartbeat":
            output = "heartbeat_ack"

        else:
            output = "Error: Invalid command"

        return output

    def send_topology_to_sock(self, sock):
        data = str(self.partitions).encode()
        sock.send(data)

    def process_request(self, conn):
        while True:
            try:
                msg = conn.recv(2048).decode()
                print(f"{msg} received")
                output = self.handle_commands(msg, conn)

                conn.send(output.encode())

            except Exception as e:
                print(e)
                print("Error processing message from client")
                conn.close()
                break
            
    def leader_heartbeat(self, interval=5, timeout=10):
        while True:
            time.sleep(interval)
            if not self.is_leader:
                leader_index = 0
                try:
                    with self.socket_locks[self.cluster_index][leader_index]:
                        if self.conns[self.cluster_index][leader_index][2] is None or self.conns[self.cluster_index][leader_index][2]._closed:
                            self.conns[self.cluster_index][leader_index][2] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            self.conns[self.cluster_index][leader_index][2].setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                            self.conns[self.cluster_index][leader_index][2].connect((self.conns[self.cluster_index][leader_index][0], self.conns[self.cluster_index][leader_index][1]))
                        
                        self.conns[self.cluster_index][leader_index][2].send("heartbeat".encode())
                        self.conns[self.cluster_index][leader_index][2].settimeout(timeout)
                        response = self.conns[self.cluster_index][leader_index][2].recv(1024).decode()
                        if response != "heartbeat_ack":
                            raise Exception("Invalid response from leader")
                except Exception as e:
                    print("Leader is not responding, electing a new leader...")
                    if self.conns[self.cluster_index][leader_index][2] is not None:
                        self.conns[self.cluster_index][leader_index][2].close()
                        self.conns[self.cluster_index][leader_index][2] = None
                        
                        print(self.conns[self.cluster_index])
                        
                    self.elect_new_leader()

    def elect_new_leader(self):
        with self.cluster_lock:
            # Identify next possible leader
            next_leader_index = (self.partitions[self.cluster_index].index(f"{self.ip}:{self.port}") + 1) % len(self.partitions[self.cluster_index])
            
            while True:
                leader_ip, leader_port = self.partitions[self.cluster_index][next_leader_index].split(':')

                if self.check_node_availability(leader_ip, leader_port):
                    break
                else:
                    next_leader_index = (next_leader_index + 1) % len(self.partitions[self.cluster_index])

            # If the next possible leader is self, become leader
            if (leader_ip, str(leader_port)) == (self.ip, str(self.port)):
                self.is_leader = True
                print("This node is now the leader")
                self.conns[self.cluster_index] = [[None]]
                self.partitions[self.cluster_index] = [f"{self.ip}:{self.port}"]

            # If the next possible leader is not self, wait for its leadership
            else:
                self.is_leader = False
                time.sleep(4) # make sure new leader is ready
                print(f"Waiting for the new leader {leader_ip}:{leader_port}...")
                self.initialize_commit_log()
                sys_argv = ["", self.ip, self.port, "REJOIN", leader_ip, leader_port, self.cluster_index]
                getTopologyAndConnect(self.ip, self.port, "REJOIN", sys_argv)
                
    def check_node_availability(self, ip, port):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((ip, int(port)))
            sock.close()
            return True
        except Exception as e:
            print(f"Node {ip}:{port} is not available: {e}")
            return False

    def listen_to_clients(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('0.0.0.0', int(self.port)))
        sock.listen(50)
        
        utils.run_thread(fn=self.leader_heartbeat, args=())

        while True:
            try:
                client_socket, client_address = sock.accept()

                print(f"Connected to new client at address {client_address}")
                my_thread = Thread(target=self.process_request, args=(client_socket,))
                my_thread.daemon = True
                my_thread.start()

            except:
                print("Error accepting connection...")


def decide_addition_strategy(ip_address, port, current_topology):
    NODES_PER_CLUSTER = 3  # number of replicas per cluster
    for cluster in current_topology:
        if len(cluster) < NODES_PER_CLUSTER:
            cluster.append(f"{ip_address}:{port}")
            return str(current_topology)

    # if no cluster has space, create new cluster
    current_topology.append([f"{ip_address}:{port}"])
    return str(current_topology)


def getTopologyAndConnect(ip_address, port, partitions, sys_argv_list):
    if partitions != "NEW" and partitions != "REJOIN":
        dht = HashTableService(ip=ip_address, port=port, partitions=partitions)
        dht.listen_to_clients()
    assert len(sys_argv_list) >= 6, "Invalid number of arguments"
    # new node with no prior knowledge of network topology
    # connect to known leader and get topology
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.connect((str(sys_argv_list[4]), int(sys_argv_list[5])))
    server.send("topology".encode())
    try:
        current_topology = eval(server.recv(2048).decode())
        print(f"Current topology: {current_topology}")
    except:
        print("Error getting topology from leader")
        sys.exit(1)
    if partitions == "NEW":
        current_topology = decide_addition_strategy(ip_address, port, current_topology)
    elif partitions == "REJOIN":
        assert len(sys_argv_list) == 7, "Invalid number of arguments"
        cluster_index = int(sys_argv_list[6])
        current_topology[cluster_index].append(f"{ip_address}:{port}")
        print(f"Rejoining cluster {cluster_index}")
        print(f"Current topology: {current_topology}")

    dht = HashTableService(ip=ip_address, port=port, partitions=str(current_topology))
    dht.listen_to_clients()


if __name__ == '__main__':
    ip_address = str(sys.argv[1])
    port = int(sys.argv[2])
    partitions = str(sys.argv[3])

    getTopologyAndConnect(ip_address, port, partitions, sys.argv)
