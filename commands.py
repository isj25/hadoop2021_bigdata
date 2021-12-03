import os
import json
from filesplit import *

config_file = open('dfs_setup_config.json','r')
config = json.load(config_file)

no_of_nodes = config['num_datanodes']
replication = config['replication_factor']
block_size = config['block_size']

datanode = os.path.expandvars(config['path_to_datanodes'])
namenode = os.path.expandvars(config['path_to_namenodes'])
fs_path = os.path.expandvars(config['fs_path'])

datanode_path = os.path.join(datanode, 'DataNodes')

datanode_tracker = open(namenode + 'datanode_tracker.json', 'r+')
datanode_details = json.load(datanode_tracker)


def put_command(source, destination):
	user_file = source.split('/')[-1]
	dest_path = os.path.join(destination, user_file)

	#mapping of virtual path and file name
	mapping_file = open(namenode + "mapping_file.json",'r+')
	mapping_data = json.load(mapping_file)

	if not destination in mapping_data:
		mapping_file.close()
		raise Exception(destination,"No such directory")

	mapping_data[destination].append(user_file)
	mapping_file.seek(0)
	json.dump(mapping_data,mapping_file,indent=4)
	mapping_file.close()

	#file to keep track of where file blocks are stored
	location_file = open(namenode + "location_file.json","r+")
	try:
		location_data = json.load(location_file)
	except Exception as e:
		location_data = {}
	location_data[dest_path] = []

	#splitting files to datanodes
	next_datanode = datanode_details['Next_datanode']
	for split in fileSplit(source,block_size):
		replica = []
		for _ in range(replication):
			DN_str = 'DN' + str(next_datanode)
			blk_no = datanode_details[DN_str][1]
			block = 'block' + str(blk_no)
			
			datanode_details[DN_str][1] += 1
			datanode_details[DN_str][0][blk_no - 1] = 1
			next_datanode = (next_datanode % no_of_nodes) + 1

			store_path = DN_str + '/' + block
			replica.append(store_path)

			file_path = os.path.join(datanode_path, store_path)
			file = open(file_path,'w')
			file.write(split)
			file.close()

		location_data[dest_path].append(replica)


	datanode_details['Next_datanode'] = next_datanode

	datanode_tracker.seek(0)
	json.dump(datanode_details, datanode_tracker, indent=4)
	datanode_tracker.close()

	location_file.seek(0)
	json.dump(location_data,location_file, indent=4)
	location_file.close()


def cat_command():
	pass


def ls_command(path):
	mapping_file = open(namenode + "mapping_file.json",'r')
	mapping_data = json.load(mapping_file)
	
	#directory doesn't exist
	if not path in mapping_data:
		mapping_file.close()
		raise Exception(path,"No such directory")

	for entry in mapping_data[path]:
		if (path + entry) in mapping_data:
			print(entry, "\tDirectory")
		else:
			print(entry, "\tfile")

	mapping_file.close()


def rmdir_command(path):
	split = os.path.split(path)
	par_path, user_dir = split[0], split[1]

	mapping_file = open(namenode + "mapping_file.json",'r+')
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
	split = os.path.split(path)
	par_path, user_dir = split[0], split[1]

	mapping_file = open(namenode + "mapping_file.json",'r+')
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
