from posix import listdir
import time
import shutil
import os
import json
from utilities import *
#from termcolor import colored

config_file = open('current_config.json','r')
config = json.load(config_file)


datanode = os.path.expandvars(config['path_to_datanodes'])
namenode = os.path.expandvars(config['path_to_namenodes'])
no_of_nodes = config['num_datanodes']
datanode_size = config['datanode_size']
sync_period = config['sync_period']




# def namenodeheart(heartbeat,DN_str):
#     if(heartbeat == 1):
#         raise Exception(DN_str,"Datanode is full")
#     else:
#         print("no error")

# def datanodeheart():
#     for d_num in range(1,no_of_nodes+1):
#         DN_str = "DataNodes/DN"+ str(d_num)
#         dnode = os.path.join(datanodes,DN_str)
#         items = os.listdir(dnode)
#         #print(items)
#         if(len(items)==datanode_size):
#             # 0 for available
#             # 1 for full
#             heartbeat = 1
#             namenodeheart(heartbeat,DN_str)
#         else:
#             heartbeat = 0
#             namenodeheart(heartbeat,DN_str)

def namenode_heartbeat():
    location_file = open(namenode+'location_file.json','r')
    location_data = json.load(location_file)
    for file in location_data.keys():
        all_blocks = location_data[file]
        for replicas in all_blocks:
            for i in range (0,len(replicas)):
                if not os.path.isfile(datanode+"DataNodes/"+replicas[i]):
                    print("Block doesn't exist, recreating",replicas[i])
                    if(i == 0):
                        while(not os.path.isfile(datanode+"DataNodes/"+replicas[i])):
                            i += 1
                        t = 0
                        while t != i:
                            shutil.copy(datanode+"DataNodes/"+replicas[i], datanode+"DataNodes/"+replicas[t])
                            t += 1
                    else:
                        shutil.copy(datanode+"DataNodes/"+replicas[i-1], datanode+"DataNodes/"+replicas[i])
    #print("everything ok")

while(True):
    time.sleep(sync_period)
    namenode_heartbeat()
