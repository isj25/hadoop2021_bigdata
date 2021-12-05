import os
import json
import datetime

def init_DFS(config_file_path = 'default_config.json'):
	config_file = open(config_file_path)
	config = json.load(config_file)

	datanode = os.path.expandvars(config['path_to_datanodes'])
	namenode = os.path.expandvars(config['path_to_namenodes'])
	DFS = os.path.expandvars(config['fs_path']) # FS PATH
	
	
	os.mkdir(datanode)   	#creating datanode
	os.mkdir(namenode)	    #creating namenode
	os.makedirs(DFS)        #root
	
	name_node_logfile_path = os.path.expandvars(config['namenode_log_path'])
	namenode_log_file = open(name_node_logfile_path,'a+')
	namenode_log_file.write(str(datetime.datetime.now()) + ": created namenode log file\n")

	mapping_file = open(namenode + 'mapping_file.json','w')
	namenode_log_file.write(str(datetime.datetime.now()) + ": created namenode mapping file\n")

	location_file = open(namenode + 'location_file.json','w')
	namenode_log_file.write(str(datetime.datetime.now()) + ": created namenode location_file\n")

	datanode_tracker = open(namenode + 'datanode_tracker.json', 'w')
	namenode_log_file.write(str(datetime.datetime.now()) + ": created namenode log file\n")

	datanode_data = {}
	datanode_data['Next_datanode'] = 1

	datanode_size = config['datanode_size']						
	blocks = [0 for _ in range(datanode_size)]				#empty blocks

	directory = 'DataNodes'
	cur_path = os.path.join(datanode, directory)
	os.mkdir(cur_path)
	
	no_of_nodes = config['num_datanodes']
	for i in range(no_of_nodes):
		dirname = 'DN' + str(i+1)
		path = os.path.join(cur_path, dirname)
		os.mkdir(path)
		namenode_log_file.write(str(datetime.datetime.now()) + ": created datanode " + dirname +"\n")
		datanode_data[dirname] = blocks

	datanode_tracker.write(json.dumps(datanode_data, indent=4))
	mapping_file.write(json.dumps({'/' : []}, indent=4))
	location_file.write(json.dumps({}, indent=4))
	
	datanode_tracker.close()
	mapping_file.close()
	location_file.close()

	dfs_setup = open("dfs_setup_config.json",'w') 
	dfs_setup.write(json.dumps(config,indent=4))
	dfs_setup.close()
	namenode_log_file.close()
init_DFS('config_sample.json')
