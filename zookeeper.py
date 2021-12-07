import time
import os
import json
from utilities import *
import shutil
#from termcolor import colored

config_file = open('current_config.json','r')
config = json.load(config_file)

namenode = os.path.expandvars(config['path_to_namenodes'])
sec_namenode = os.path.expandvars(config['secondary_namenode_path'])
sync = config['sync_period']

while(True):
    time.sleep(sync)
    if(not os.path.isdir(namenode)):
        shutil.copytree(sec_namenode,namenode)
        print("namenode retrived from backup")
    else:
        print("everything ok")
