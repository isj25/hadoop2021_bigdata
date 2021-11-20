import os
import json

def init_DFS(config_file_path = ''):
	if config_file_path == '':
		config_file_path = 'default_config.json'

	config_file = open(config_file_path)
	config = json.load(config_file)
	print(json.dumps(config, indent=4))


init_DFS('config_sample.json')
