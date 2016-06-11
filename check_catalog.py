#!/usr/bin/python3

import sys
import os

def lister(root):
	result = []
	for (thisdir, subshere, fileshere) in os.walk(root):
		result.append(thisdir)
	for fname in fileshere:
		path=os.path.join(thisdir, fname)
		result.append(path)
	return result

if __name__=='__main__':
	lister(sys.argv[1])
