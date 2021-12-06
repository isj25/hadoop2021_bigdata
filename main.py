import argparse
from commands import *
from shutil import copytree

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Command line interface for YAH')
	parser.add_argument('command', choices=['put', 'cat', 'ls', 'rm', 'mkdir', 'rmdir','mapreduce'])
	parser.add_argument('--arg1')
	parser.add_argument('--arg2')
	parser.add_argument('--input')
	parser.add_argument('--output')
	parser.add_argument('--config')
	parser.add_argument('--mapper')
	parser.add_argument('--reducer')

	args = parser.parse_args()
	command = args.command

	if command == 'put':
		source = args.arg1
		destination = args.arg2
		put_command(source,destination)
	elif command == 'cat':
		path = args.arg1
		cat_command(path)
	elif command == 'ls':
		path = args.arg1
		ls_command(path)
	elif command == 'rm':
		path = args.arg1
		rm_command(path)
	elif command == 'mkdir':
		path = args.arg1
		mkdir_command(path)
	elif command == 'rmdir':
		path = args.arg1
		rmdir_command(path)
	elif command == 'mapreduce':
		fs_input = args.input
		fs_output = args.ouput
		config_path = args.config
		abs_mapper = args.mapper
		abs_reducer = args.reducer
		mapreducejob(fs_input,fs_output,config_path,abs_mapper,abs_reducer)
		
	copytree(namenode, secondary_namenode_path, dirs_exist_ok=True)
