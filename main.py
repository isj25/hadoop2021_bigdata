import argparse

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('command', choices=['put', 'cat', 'ls', 'rm', 'mkdir', 'rmdir'])
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
