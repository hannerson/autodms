#-*- coding:utf-8 -*-


from pooldb import *
import hashlib
import traceback
import urllib2
import json
import time
import requests
import ConfigParser
import sys

sys.setdefaultencoding('utf-8')
reload(sys)

g_config_parser = ConfigParser.ConfigParser()
g_config_parser.read("./config/config.conf")

def checkValid(timestamp,digest):
	try:
		#if g_config_parser.has_option('common','private_key'):
		#	private_key = g_config_parser.get('common','private_key')
		#else:
		#	return False
		if g_config.configinfo.has_key("common") and g_config.configinfo["common"].has_key("private_key"):
			private_key = g_config.configinfo["common"]["private_key"]
		else:
			return False
		hl = hashlib.md5()
		hl.update(str("%s_%s" % (private_key,timestamp)).encode(encoding='utf-8'))
		if digest == h1.hexdigest():
			return True
		else:
			return False
	except Exception,e:
		traceback.print_exc()

def client(url):
	try:
		if g_config_parser.has_option('common','private_key'):
			private_key = g_config_parser.get('common','private_key')
		else:
			return False
		hl = hashlib.md5()
		t = int(time.time())
		hl.update(str("%s_%s" % (private_key,t)).encode(encoding='utf-8'))
		digest = hl.hexdigest()
		
		post_dict = {}
		post_dict["time"] = "%s" % t
		post_dict["pkey"] = "%s" % digest
		#data = {"1":"1","2":"2","3":"3"}
		#post_dict["data"] = data

		post_data = json.dumps(post_dict)
		print post_data
		f = urllib2.urlopen(
			url = url,
			data = post_data
		)
		result = f.read()
		print result
	except Exception,e:
		traceback.print_exc()

def client2(url,id_type):
	try:
		#if g_config_parser.has_option('common','private_key'):
		#	private_key = g_config_parser.get('common','private_key')
		#else:
		#	return False
		if g_config.configinfo.has_key("common") and g_config.configinfo["common"].has_key("private_key"):
			private_key = g_config.configinfo["common"]["private_key"]
		else:
			return False
		hl = hashlib.md5()
		t = int(time.time())
		hl.update(str("%s%s" % (t,private_key)).encode(encoding='utf-8'))
		digest = hl.hexdigest()
		
		post_dict = {}
		post_dict["client_time"] = "%s" % t
		post_dict["check_sum"] = "%s" % digest
		post_dict["callback"] = "%s" % "http://abcd/"
		post_dict["dmscode"] = "%s" % 1
		post_dict["srcid"] = "%s" % "12345adfdf"
		post_dict["tasktype"] = 0
		data = {
			"id_type":id_type,
			"uni_id":123,
			"global_id":309227020,
			"musician_id":1,
		}

		#post_dict["data"] = str(data)
		post_dict["data"] = data
		headers = {"Content-Type":"application/json;charset:utf-8;"}
		post_data = json.dumps(post_dict)
		print post_data
		f = requests.post(
			url = url,
			headers = headers,
			data = post_data
		)
		print f.text
		#result = json.dumps
	except Exception,e:
		traceback.print_exc()

def client3(url,id_type):
	try:
		#if g_config_parser.has_option('common','private_key'):
		#	private_key = g_config_parser.get('common','private_key')
		#else:
		#	return False
		if g_config.configinfo.has_key("common") and g_config.configinfo["common"].has_key("private_key"):
			private_key = g_config.configinfo["common"]["private_key"]
		else:
			return False
		hl = hashlib.md5()
		t = int(time.time())
		hl.update(str("%s%s" % (t,private_key)).encode(encoding='utf-8'))
		digest = hl.hexdigest()
		
		post_dict = {}
		post_dict["client_time"] = "%s" % t
		post_dict["check_sum"] = "%s" % digest
		post_dict["dmscode"] = "%s" % 0
		post_dict["srcid"] = "%s" % "5da1af1b2afed9e7429a8835"
		post_dict["tasktype"] = id_type

		#post_dict["data"] = str(data)
		headers = {"Content-Type":"application/json;charset:utf-8;"}
		post_data = json.dumps(post_dict)
		print post_data
		f = requests.post(
			url = url,
			headers = headers,
			data = post_data
		)
		print f.text
		#result = json.dumps
	except Exception,e:
		traceback.print_exc()

if __name__ == '__main__':
	#client2("http://10.0.71.8:5020/autodms?m=task&a=create",1)
	#client2("http://tmetest.kuwo.cn/autodms?m=task&a=create",1)
	#client3("http://10.0.71.8:6021/autodms/query",3)
	client3("http://tmeserve.kuwo.cn/autodms/query",3)
	#client2("http://10.0.71.8:5007/unibang?m=tmeMusician&a=relationBuild",3)
	#client2("http://10.0.71.8:5007/unibang?m=tmeMusician&a=relationBuild",4)
