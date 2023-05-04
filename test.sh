#!/bin/bash
rm -rf commit-log*
start "hash table 1" python "hashtable_service.py" "127.0.0.1" "5001" "[['127.0.0.1:5001']]"
sleep 2
start "hash table 2" python "hashtable_service.py" "127.0.0.1" "5002" "[['127.0.0.1:5001', '127.0.0.1:5002']]"
#sleep 2
#start "hash table 3" python "hashtable_service.py" "127.0.0.1" "5003" "[['127.0.0.1:5001', '127.0.0.1:5002'],['127.0.0.1:5003']]"
