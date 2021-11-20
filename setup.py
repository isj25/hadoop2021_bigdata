import os
import json
from fsplit.filesplit import Filesplit

fs = Filesplit()

def init_DFS(config_file_path = 'default_config.json'):
	config_file = open(config_file_path)
	config = json.load(config_file)

	USER = os.path.expanduser('~')
	parent_path = f'{USER}/DATANODE/'
	
	os.mkdir(parent_path)
	
	directory = 'DataNodes'
	cur_path = os.path.join(parent_path, directory)
	os.mkdir(cur_path)
	
	no_of_nodes = config["datanode_size"]
	for i in range(no_of_nodes):
		dirname = 'DN'+str(i)
		path = os.path.join(cur_path, dirname)
		os.mkdir(path)

	dirname = 'temp'
	path = os.path.join(cur_path, dirname)
	os.mkdir(path)

	block_size = config["block_size"]

	temp_path = os.path.join(cur_path, 'temp/')
	src_file = f'{USER}/sample.json'

	fs.split(file=src_file, split_size=block_size, output_dir=temp_path)
	  
	allfiles = os.listdir(temp_path)
	
	for index,file in enumerate(allfiles):
	    os.rename(temp_path + file, cur_path + '/DN' + str(index%no_of_nodes) + '/' + file)

init_DFS('config_sample.json')
