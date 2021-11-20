from fsplit.filesplit import Filesplit
import json
fs = Filesplit()

# def split_cb(f, s):
#     print("file: {0}, size: {1}".format(f, s))
config_file =open("config_sample.json",)
file_data = json.load(config_file)

size = file_data['block_size']
size = size * 1000000
print(size)
fs.split(file="/home/pes1ug19cs191/Desktop/100MB.bin", split_size=size, output_dir="/home/pes1ug19cs191/Desktop/test")