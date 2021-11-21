import os.path
from math import ceil

def fileSplit(path, splitSize):
	file = open(path, 'r')
	size = os.path.getsize(path)

	blocks = ceil(size / splitSize)

	for _ in range(blocks):
		yield file.read(splitSize)
