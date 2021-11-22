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
fs_path = os.path.expandvars(config['fs_path'])

def put_command(source, destination):
	block_size = config["block_size"]
	user_file = source.split('/')[-1]

	#mapping fs_path files
	mapping_file = open(namenode+"mapping_file.json",'r+')
	mapping_data = json.load(mapping_file)

	if not destination in mapping_data:
		mapping_file.close()
		raise Exception(destination,"No such directory")
	mapping_data[destination].append(user_file)
	mapping_file.seek(0)
	json.dump(mapping_data,mapping_file,indent=4)
	mapping_file.close()

	#file to keep track of where file blocks are stored
	location_file = open(namenode+"location_file.json","r+")
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

def ls_command(path):
	mapping_file = open(namenode+"mapping_file.json",'r')
	mapping_data = json.load(mapping_file)
	
	#directory doesn't exist
	if not path in mapping_data:
		mapping_file.close()
		raise Exception(path,"No such directory")
	for entry in mapping_data[path]:
		if (path+'/'+entry) in mapping_data:
			print(entry, "\tDirectory")
		else:
			print(entry, "\tfile")
	mapping_file.close()



def rmdir_command(path):
	path_list = path.split('/')
	user_dir = path_list.pop()
	par_path = '/'.join(path_list)

	mapping_file = open(namenode+"mapping_file.json",'r+')
	mapping_data = json.load(mapping_file)
	
	#directory doesn't exist
	if not path in mapping_data:
		mapping_file.close()
		raise Exception(path,"No such directory")

	if len(mapping_data[path]) != 0:
		mapping_file.close()
		raise Exception(path,"Directory is not empty")

	del mapping_data[path]
	
	#deleting entry in parent directory
	if par_path in mapping_data:
		mapping_data[par_path].remove(user_dir)

	mapping_file.truncate(0)
	mapping_file.seek(0)
	json.dump(mapping_data,mapping_file,indent=4)
	mapping_file.close()


def mkdir_command(path):
	path_list = path.split('/')
	user_dir = path_list.pop()
	par_path = '/'.join(path_list)
	mapping_file = open(namenode+"mapping_file.json",'r+')
	mapping_data = json.load(mapping_file)
	
	#have to create parent directory before creating subdirectory
	if not par_path in mapping_data:
		mapping_file.close()
		raise Exception(par_path,"No such directory")

	#appends subdirectory to parent directory
	mapping_data[par_path].append(user_dir)
	mapping_data[path] = []
	mapping_file.seek(0)
	json.dump(mapping_data,mapping_file,indent=4)
	mapping_file.close()

def rm_command():
	pass
