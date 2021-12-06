import time
import os
import json
from utilities import *
#from termcolor import colored

config_file = open('current_config.json','r')
config = json.load(config_file)

namenode = os.path.expandvars(config['path_to_namenodes'])
sec_namenode = os.path.expandvars(config['secondary_namenode_path'])
while(True):
    if(not os.path.isdir(namenode)):
        os.mkdir(namenode)
        os.rename(sec_namenode,namenode)
        os.mkdir(sec_namenode)