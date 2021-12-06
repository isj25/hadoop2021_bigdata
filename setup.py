import os
import sys
import json
import datetime
from utilities import updateJSON 

def init_DFS(config_file_path):
	config_file = open(config_file_path)
	config = json.load(config_file)

	datanode = os.path.expandvars(config['path_to_datanodes'])
	namenode = os.path.expandvars(config['path_to_namenodes'])
	datanode_logs = os.path.expandvars(config['datanode_log_path'])
	
	if os.path.isdir(datanode):
		raise Exception("hdfs already exists")

	os.mkdir(datanode)   	#creating datanode
	os.mkdir(namenode)	    #creating namenode
	os.mkdir(datanode_logs)
	
	#name node log file
	name_node_logfile_path = os.path.expandvars(config['namenode_log_path'])
	namenode_log_file = open(name_node_logfile_path,'a+')
	namenode_log_file.write(str(datetime.datetime.now()) + " : created namenode log file\n")

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
		logpaths = os.path.join(datanode_logs,dirname)												#create log file for each datanode
		logpaths = logpaths +".txt"
		datanode_logfiles = open(logpaths,'a+')
		datanode_logfiles.write(str(datetime.datetime.now()) + " : created datanode " + dirname + "\n")
		datanode_logfiles.close()

		datanode_data[dirname] = blocks

	datanode_tracker.write(json.dumps(datanode_data, indent=4))
	mapping_file.write(json.dumps({'/' : []}, indent=4))
	location_file.write(json.dumps({}, indent=4))
	
	datanode_tracker.close()
	mapping_file.close()
	location_file.close()
	namenode_log_file.close()

	hdfs_file = open('hdfs.json','r+')
	hdfs_data = json.load(hdfs_file)
	hdfs_data["no_of_dfs"] = hdfs_data["no_of_dfs"] + 1

	dfs_setup = open("dfs_setup_config_" + str(hdfs_data["no_of_dfs"])+".json",'w')
	hdfs_main_path  = os.path.split(os.path.split(datanode)[0])[0]
	config["secondary_namenode_path"] = hdfs_main_path + "/SECONDARYNAMENODE"

	os.mkdir(config["secondary_namenode_path"])

	dfs_setup.write(json.dumps(config,indent=4))
	dfs_setup.close()
	updateJSON(hdfs_data,hdfs_file)


if len(sys.argv) > 1:
	config = sys.argv[1]
else:
	config = 'default_config.json'

init_DFS(config)
