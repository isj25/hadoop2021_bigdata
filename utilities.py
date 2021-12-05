
import os
import json
from math import ceil


def fileSplit(path, splitSize):
	file = open(path, 'r')
	size = os.path.getsize(path)

	blocks = ceil(size / splitSize)

	for _ in range(blocks):
		yield file.read(splitSize)



def updateJSON(data, file):
	file.seek(0)
	file.truncate(0)
	json.dump(data, file, indent=4)
	file.close()
