import sys, socket, pickle
from threading import Thread
import random, string, time


def get_socket():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    return sock


if len(sys.argv) != 3:
    print("Correct usage: script, IP address, port number")
    exit()

server_ip_address = str(sys.argv[1])
server_port = int(sys.argv[2])

server = get_socket()
server.connect((server_ip_address, server_port))


def listen_for_messages():
    while True:
        output = server.recv(2048).decode()
        print(output)


s = string.ascii_lowercase
request_id = 0

key = ''.join(random.sample(s, random.randint(1, 5)))
val = random.randint(1, 100000)
req_id = int(time.time() * 1000)

# set
command = f"set {key} {val}"  # input()
print(command)
command = command + ' ' + str(req_id)
server.send(command.encode())
resp = server.recv(2048).decode()
print(resp)
set_request_id = req_id

# get
req_id = int(time.time() * 1000)
command = f"get {key}"  # input()
print(command)
command = command + ' ' + str(req_id)
server.send(command.encode())
resp = server.recv(2048).decode()
print(resp)


command = "topology"  # input()
print(command)
server.send(command.encode())
resp = server.recv(2048).decode()
print(resp)

