#-*- coding:utf-8 -*-
###config class###

import threading

class config(object):
	_instance_lock = threading.Lock()
	def __init__(self,path):
		self.path = path
		self.configinfo = self.loadConfig()

	def __new__(cls, *args, **kwargs):
		if not hasattr(config, "_instance"):
			with config._instance_lock:
				if not hasattr(config, "_instance"):
					config._instance = object.__new__(cls)
		return config._instance

	def loadConfig(self):
		fieldConfig = {}
		currentField = ""
		f = open(self.path,"r")
		for line in f:
			if line.strip() == "" or line.strip()[0] == "#":
				continue
			if line.strip()[0] == "[" and line.strip()[-1] == "]":
				field = line.strip()[1:-1]
				currentField = field
				if fieldConfig.has_key(field):
					continue
				else:
					fieldConfig[field] = {}
			elif line.strip().find("=") != -1:
				arr = line.strip().split("=")
				if len(arr) > 2:
					arr[1] = "=".join(arr[1:])
				if arr[1] == "":
					continue
				if arr[1].isdigit():
					fieldConfig[currentField][arr[0]] = int(arr[1])
				else:
					fieldConfig[currentField][arr[0]] = arr[1]
					
			else:
				continue
		f.close()
		return fieldConfig

