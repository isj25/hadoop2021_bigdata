import time
import shutil
import os
import json
from utilities import *

config_file = open('current_config.json','r')
config = json.load(config_file)


datanode = os.path.expandvars(config['path_to_datanodes'])
namenode = os.path.expandvars(config['path_to_namenodes'])
no_of_nodes = config['num_datanodes']
datanode_size = config['datanode_size']
sync_period = config['sync_period']
num_datanodes = config['num_datanodes']


def namenode_heartbeat():
    while(True):
        #to check for namenode failure
        try:
            location_file = open(namenode+'location_file.json','r')
            location_data = json.load(location_file)
            break
        except:
            pass
    #handle datanode failure
    for dir_no in range(1,num_datanodes+1):
        if not os.path.isdir(datanode+"DataNodes/DN"+str(dir_no)):
            os.mkdir(datanode+"DataNodes/DN"+str(dir_no))

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
    print("everything ok")

while(True):
    time.sleep(sync_period)
    namenode_heartbeat()
