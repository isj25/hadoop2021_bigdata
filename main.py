import argparse

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Command line interface for YAH')
	parser.add_argument('command', choices=['put', 'cat', 'ls', 'rm', 'mkdir', 'rmdir'])
	parser.add_argument('--arg1')
	parser.add_argument('--arg2')
	
	args = parser.parse_args()
	command = args.command

	if command == 'put':
		pass
	elif command == 'cat':
		pass
	elif command == 'ls':
		pass
	elif command == 'rm':
		pass
	elif command == 'mkdir':
		pass
	elif command == 'rmdir':
		pass
