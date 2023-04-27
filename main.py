import time

from dhtnode import DHTNode

node_1 = DHTNode("127.0.0.1", 10001, 1)
node_2 = DHTNode("127.0.0.1", 10002, 2)
node_1.debug = True
node_2.debug = True

time.sleep(2)

node_1.start()
node_2.start()

time.sleep(2)

print(node_1.connect_with_node('127.0.0.1', 10002))

time.sleep(20)

print(f"Inbound %s", list(node_1.nodes_inbound))
print(f"Outbound %s", list(node_1.nodes_outbound))



