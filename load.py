import os
import json
import sys
from utilities import updateJSON

if (len(sys.argv) > 1):
    setup_config_path = sys.argv[1]
    try:
        config = open(setup_config_path,'r+')
        cur_config = open('current_config.json','w')
        config_data = json.load(config)
        updateJSON(config_data,cur_config)
        print(config_data)
        config.close()
    except Exception:
        print("No such file exists, create a new config file")

config = open('current_config.json','r')
config_data = json.load(config)
datanode = os.path.expandvars(config['path_to_datanodes'])
namenode = os.path.expandvars(config['path_to_namenodes'])
if os.path.isdir(datanode) and os.path.isdir(namenode):
    print("Successfully loaded DFS")
else:
    print("Failed to load DFS")
