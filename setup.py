import os
import json

def init_DFS(config_file_path = 'default_config.json'):
	config_file = open(config_file_path)
	config = json.load(config_file)
	print(json.dumps(config, indent=4))

	parent_path = '/home/pes1ug19cs438/Desktop/BD_assignments/Project/hadoop2021_bigdata-main/'
	directory = 'DataNodes'
	cur_path = os.path.join(parent_path, directory)
	os.mkdir(cur_path)
	
	no_of_nodes = config["datanode_size"]
	for i in range(no_of_nodes):
		dirname = 'DN'+str(i+1)
		path = os.path.join(cur_path, dirname)
		os.mkdir(path)

init_DFS('config_sample.json')
