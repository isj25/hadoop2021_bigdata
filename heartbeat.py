from posix import listdir
import time

import os
import json
from utilities import *
#from termcolor import colored

config_file = open('dfs_setup_config.json','r')
config = json.load(config_file)


datanodes = os.path.expandvars(config['path_to_datanodes'])
no_of_nodes = config['num_datanodes']
datanode_size = config['datanode_size']




def namenodeheart(heartbeat,DN_str):
    if(heartbeat == 1):
        raise Exception(DN_str,"Datanode is full")
    else:
        print("no error")

def datanodeheart():
    for d_num in range(1,no_of_nodes+1):
        DN_str = "DataNodes/DN"+ str(d_num)
        dnode = os.path.join(datanodes,DN_str)
        items = os.listdir(dnode)
        #print(items)
        if(len(items)==datanode_size):
            # 0 for available
            # 1 for full
            heartbeat = 1
            namenodeheart(heartbeat,DN_str)
        else:
            heartbeat = 0
            namenodeheart(heartbeat,DN_str)
while(True):
    time.sleep(3)
    datanodeheart()
