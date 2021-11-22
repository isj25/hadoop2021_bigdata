import os
import json
from filesplit import *

config_file = open('dfs_setup_config.json','r')
config = json.load(config_file)

datanode = os.path.expandvars(config['path_to_datanodes'])
namenode = os.path.expandvars(config['path_to_namenodes'])
datanode_path = datanode+'DataNodes/'

no_of_nodes = config['num_datanodes']
replication = config['replication_factor']
    
def put_command(source, destination):
	block_size = config["block_size"]
	user_file = source.split('/')[-1]

	#mapping fs_path files
	mapping_file = open(namenode+"mapping_file.json",'w+')
	try:
		mapping_data = json.load(mapping_file)
	except Exception as e:
		mapping_data = {}
	mapping_data[destination] = user_file
	mapping_file.seek(0)
	json.dump(mapping_data,mapping_file,indent=4)
	mapping_file.close()

	#file to keep track of where file blocks are stored
	location_file = open(namenode+"location_file.json","w+")
	try:
		location_data = json.load(location_file)
	except Exception as e:
		location_data = {}
	location_data[user_file] = []
	

	#splitting files to datanodes
	i = 0
	for split in fileSplit(source,block_size):
		cur_hash = (i % no_of_nodes) + 1
		replica = []
		r = 0
		#create replication_factor number of replicas
		for j in range(cur_hash, cur_hash + replication):
			block = user_file + '_block' + str(i) + '_r' + str(r)
			store_path = 'DN' + str((j % no_of_nodes) + 1) + '/' + block
			replica.append(store_path)

			file_path = datanode_path + store_path
			file = open(file_path,'w')
			file.write(split)
			file.close()
			r = r + 1

		location_data[user_file].append(replica)
		i = i + 1

	location_file.seek(0)
	json.dump(location_data,location_file,indent=4)
	location_file.close()


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
