# -*- coding=utf-8 -*-
import os,sys
import MySQLdb
import traceback
from pooldb import *
from logger import *
import time
import json

reload(sys)
sys.setdefaultencoding('utf-8')

conn = g_pool_Src.connection()
cur = conn.cursor()

f = open(sys.argv[1],"r")
fout = open(sys.argv[1]+".xls","w+")

for line in f:
	srid = line.strip().split("\t")[0]
	sql = '''select * from AutoTask where srcid="%s" and type=3''' % (srid)
	cnt = cur.execute(sql)
	if cnt > 0:
		ret = cur.fetchone()
		content = ret["content"]
		kw_id = ret["kw_id"]
		data = json.loads(content)
		if data.has_key("info"):
			for info in data["info"]:
				if info.has_key("Artist"):
					name = info["Artist"]["m_name"]
					intro = info["Artist"]["basic_intro"]
				if info.has_key("PicSource"):
					path = info["PicSource"]["file_path"]
			fout.write("%s\t%s\t%s\t%s\t%s\n" % (srid,name,intro,path,kw_id))
fout.close()
f.close()

cur.close()
conn.close()
