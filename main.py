import argparse
from commands import *

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Command line interface for YAH')
	parser.add_argument('command', choices=['put', 'cat', 'ls', 'rm', 'mkdir', 'rmdir'])
	parser.add_argument('--arg1')
	parser.add_argument('--arg2')
	
	args = parser.parse_args()
	command = args.command

	if command == 'put':
		source = args.arg1
		destination = args.arg2
		put_command(source,destination)
	elif command == 'cat':
		pass
	elif command == 'ls':
		path = args.arg1
		ls_command(path)
	elif command == 'rm':
		pass
	elif command == 'mkdir':
		path = args.arg1
		mkdir_command(path)
	elif command == 'rmdir':
		path = args.arg1
		rmdir_command(path)
