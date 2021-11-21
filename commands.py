import os
import json
from filesplit import *

config_file = open('dfs_setup_config.json','r')
config = json.load(config_file)

datanode = os.path.expandvars(config['path_to_datanodes'])
datanode_path = datanode+'DataNodes/'
no_of_nodes = config['num_datanodes']

def put_command(source, destination):
	block_size = config["block_size"]
	i = 0
	for split in fileSplit(source,64):
		cur_hash = str((i % no_of_nodes) + 1)
		block = 'block' + str(i+1)
		file_path = datanode_path + 'DN' + cur_hash + '/' + block
		file = open(file_path,'w')
		file.write(split)
		i = i + 1

def cat_command():
	pass

def ls_command():
	pass

def rmdir_command():
	pass

def mkdir_command():
	pass

def rm_command():
	pass