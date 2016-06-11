#!/usr/bin/python3

import os
import argparse
import shutil
import time_compare
import check_catalog
import datetime
import time
from minio import Minio
from minio.error import ResponseError

parser = argparse.ArgumentParser()
parser.add_argument('path', type=str, help='You programm track')
parser.add_argument('quit', type=str, help='Exit key')
parser.add_argument('--s3', type=str, help='Your host')
parser.add_argument('--access_key', type=str, help='Your access key')
parser.add_argument('--secret_key', type=str, help='Your secret key')
parser.add_argument('--dir', type=str, help='Folder directory')
parser.add_argument('--bucket', type=str, help='Name of you bucket')
args = parser.parse_args()
minioClient = Minio(args.s3, access_key=args.access_key, secret_key=args.secret_key, secure=False)

def all_objects(bucket):
	objects = minioClient.list_objects(bucket, prefix='', recursive=True)
	return objects

def load_object(bucket, obj):
	try:
		minioClient.fget_object(bucket, obj, args.dir+'/'+obj)
	except ResponseError as err:
		print(err)

def upload_object(bucket, obj):
	try:
		file_stat = os.stat(obj)
		file_data = open(obj, 'rb')
		minioClient.put_object(bucket, obj, file_data, file_stat.st_size)
	except ResponseError as err:
		print(err)
"""
# Put a file with 'application/csv'.
	try:
		file_stat = os.stat('my-testfile.csv')
		file_data = open('my-testfile.csv', 'rb')
		minioClient.put_object('sync', 'huggo', file_data,file_stat.st_size, content_type='application/csv')
	except ResponseError as err:
		print(err)
"""

def get_hash(bucket, obj): 
	try:
		res = minioClient.stat_object(bucket, obj)
		return res
	except ResponseError as err:
		print(err)

def remove_object(bucket, obj):
	try:
		minioClient.remove_object(bucket, obj)
	except ResponseError as err:
		print(err)

def remove_bucket(bucket):
	try:
		minioClient.remove_bucket(bucket)
	except ResponseError as err:
		print(err)

def check_bucket(bucket):
	try:
		print(minioClient.bucket_exists(bucket))
	except ResponseError as err:
		print(err)

def all_buckets():
	buckets = minioClient.list_buckets()
	return buckets

def create_bucket(bucket):
	try:
		minioClient.make_bucket(bucket, location="us-east-1")
	except ResponseError as err:
		print(err)

def syncron_loc_web(bucket):
	try:
		res = minioClient.bucket_exists(bucket)
		path = args.dir+'/'
		if res == True:
			check_analogs_loc_serv(bucket)
			os.system('s3cmd sync {0} s3://{1}'.format(path, bucket))	
		else:
			os.system('s3cmd mb s3://{0}'.format(bucket))
			os.system('s3cmd sync {0} s3://{1}'.format(path, bucket))			
	except ResponseError as err:
		print(err)

def syncron_web_loc(bucket):
	try:
		res = minioClient.bucket_exists(bucket)
		path = args.dir+'/'
		if res == True:
			check_analogs_serv_loc(bucket)
			os.system('s3cmd sync  s3://{0} {1}'.format(bucket, path))	
		else:
			os.system('s3cmd mb s3://{0}'.format(bucket))
			os.system('s3cmd sync s3://{0} {1}'.format(bucket, path))				
	except ResponseError as err:
		print(err)

def check_analogs_loc_serv(bucket):
	res1 = []
	result_from_bucket_wgen = []
	i = 0
	result_from_bucket = all_objects(bucket)
	for obj in result_from_bucket:
		result_from_bucket_wgen.append(obj.object_name)
		res1.append(args.dir+'/'+obj.object_name)
		i += 1
	result_from_local = check_catalog.lister(args.dir)
	i = 0
	for obj in res1:
		if obj not in result_from_local:
			remove_object(bucket, result_from_bucket_wgen[i])
		i += 1

def check_analogs_serv_loc(bucket):
	res1 = []
	result_from_bucket_wgen = []
	i = 0
	result_from_bucket = all_objects(bucket)
	for obj in result_from_bucket:
		result_from_bucket_wgen.append(obj.object_name)
		res1.append(args.dir+'/'+obj.object_name)
		i += 1
	result_from_local = check_catalog.lister(args.dir)
	i = 0
	for obj in result_from_local:
		if obj not in res1:
			if args.dir != obj:
				try:
					if os.path.isdir(obj):
						pass
					else:
						os.remove(obj)
				except Exception:
					pass
		i += 1

def find_last_modified(bucket): #!!!
	data_time = []
	real_obj = []
	all_obj = all_objects(bucket)
	try:
		for obj in all_obj:
			real_obj.append(obj)
		for obj in real_obj:
			data_time.append(obj.last_modified)
			print(obj.object_name)
		return max(data_time)
	except Exception:
		return -1

def change_time(dt2):   #optionally
	pos_start = str(dt2).find(' ')+1
	pos_fin = str(dt2).find(':')
	res = str(dt2)[pos_start:pos_fin]
	try:
		res = int(res)
		if res < 21:
			res += 3
			tmp = str(dt2)[0:pos_start]+str(res)+str(dt2)[pos_fin:]
			return tmp
		else:
			res += 3
			res = res - 24
			tmp = str(dt2)[0:pos_start]+str(res)+str(dt2)[pos_fin:]
			return tmp				
	except Exception:
		return -1


def timecompare(dir1, bucket):
	dt2=find_last_modified(bucket)
	if dt2 == -1:
		return True
	else:
		dt2 = change_time(dt2) #optionally
		if dt2 != -1:
			res1 = os.popen("find {0} -printf '%TY-%Tm-%Td %TT\n' | sort -r".format(dir1)).readlines()
			dt1 = time.strptime(res1[0][0:-12], "%Y-%m-%d %H:%M:%S")
			dt2 = time.strptime(dt2[0:-13], "%Y-%m-%d %H:%M:%S")
			return (dt1 > dt2)
		else:
			return True

def action(bucket):
	if timecompare(args.dir, bucket) == True:
		syncron_loc_web(bucket)
		print('loc-web')
	else:
		syncron_web_loc(bucket)
		print('web-loc')

def run():
	res = minioClient.bucket_exists(args.bucket)
	if res == True:
		action(args.bucket)
	else:
		os.system('s3cmd mb s3://{0}'.format(args.bucket))
		action(args.bucket)

#nohup ./sub_main.py quit q --s3 127.0.0.1:9000 --access_key 4JPG1Y09DFAWJRUD7DSV --secret_key 1oqJHBuMQ2KH+KDBxA95S+2I2H8HrEXBP47uTIN8 --dir ~/a --pid /tmp/test1.pid --bucket sync &		

if __name__ == '__main__':
	run()	
