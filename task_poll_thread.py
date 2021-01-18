#!/usr/local/bin/python
# -*- coding=utf-8 -*-

import os,sys
import MySQLdb
import traceback
import utils
import json
import MySQLdb.cursors
import time
import urllib2
from pooldb import *
from logger import *
import Queue
import threading
import requests
from sqlClass import *
import signal
import hashlib

reload(sys)
sys.setdefaultencoding('utf-8')

mypid = os.getpid()
mypid_file = "run/%s.pid" % (os.path.basename(sys.argv[0]).rstrip(".py"))
f = open(mypid_file,"w+")
f.write("%s" % (mypid))
f.close()

g_process_queue = Queue.Queue(30000);

def get_realId_from_task(taskid,table,conn,cur):
	table_id = 0
	status = "none"
	reason = ""
	sql = '''select table_id,status,reason from DMSTask.Action where task_id=%s and `table`="%s" limit 1''' % (taskid,table)
	cnt = cur.execute(sql)
	print sql
	if cnt > 0:
		ret = cur.fetchone()
		table_id = ret[0]
		status = ret[1]
		reason = ret[2]
		conn.commit()
	return table_id,status,reason

def callback(origin_id,kw_id,tasktype,callback_url,code,msg, callback_key=None):
	try:
		info = {}
		info["origin_id"] = "%s" % (origin_id)
		info["kw_id"] = kw_id
		info["tasktype"] = tasktype
		info["message"] = "%s" % (msg)
		info["code"] = code
		post_json = json.dumps(info)
		logging.info(str(post_json))
		headers = {"Content-Type":"application/json"}
		hl = hashlib.md5()
		t = int(time.time())
		digest = hl.hexdigest()
		if callback_key is not None:
			hl.update(str("%s%s" % (t,callback_key)).encode(encoding='utf-8'))
			digest = hl.hexdigest()
			callback_url = "%s?sign=%s&timil=%s" % (callback_url, digest, t)
		logging.info(callback_url)
		f = requests.post(
			url = "%s" % (callback_url),
			headers = headers,
			data = post_json
		)
		result = f.text
		print result
		logging.info(str(result))
		js_ret = json.loads(result)
		task_id = 0
		if js_ret.has_key("code") and int(js_ret["code"]) == 200:
			task_id = int(js_ret["code"])
		#'''
		return task_id
	except Exception,e:
		logging.info(str(e))
		traceback.print_exc()
		return 0

def task_dispatch(conn, cur):
	mids = set()
	taskids = {}
	sql = '''select id,kw_id,srcid,taskid,callback_url,type,reason,status from AutoTask where status in (%s,%s) order by priority desc limit 5000''' % (g_status["task_send"],g_status["has_matched"])
	cnt = cur.execute(sql)
	if cnt > 0:
		ret = cur.fetchall()
		conn.commit()
		for r in ret:
			if not taskids.has_key(r["taskid"]):
				taskids[r["taskid"]] = []
			taskids[r["taskid"]].append((r["id"],r["callback_url"]))
			while g_process_queue.full():
				logging.info("sleep 2s")
				time.sleep(2)
			g_process_queue.put((r["taskid"],r["id"],r["callback_url"],r["type"],r["srcid"],r["reason"],r["kw_id"],r["status"]))
	return taskids

def task_dispatch_single(id,conn, cur):
	info = {}
	sql = '''select id,kw_id,srcid,taskid,callback_url,type,reason,status from AutoTask where id=%s''' % (id)
	cnt = cur.execute(sql)
	if cnt > 0:
		ret = cur.fetchone()
		conn.commit()
		info = ret
	return info

def update_task_poll():
	try:
		g_connSrc = g_pool_Src.connection()
		g_curSrc = g_connSrc.cursor()

		g_connRes = g_pool_Res.connection()
		g_curRes = g_connRes.cursor()

		g_connRun = g_pool_Run.connection()
		g_curRun = g_connRun.cursor()

		g_connTask = g_pool_Task.connection()
		g_curTask = g_connTask.cursor()

		task_type_map = {1:"Music",2:"Album",3:"Artist"}
		sql_src_class = sqlClass(g_connSrc,g_curSrc)
		callback_key = g_config.configinfo["common"]["callback_key"]
		global g_mutex
		while not g_process_queue.empty():
			g_mutex.acquire()
			if not g_process_queue.empty():
				ret = g_process_queue.get(False,5)
				g_mutex.release()
			else:
				g_mutex.release()
				break
			taskComplete = set()
			taskid,id,callback_url,task_type,srcid,reason,kw_id,status = ret
			if taskid == 1:
				###update status
				fields = {"status":g_status["task_suc"]}
				where = "id=%s" % (id)
				sql_src_class.mysqlUpdate("AutoTask",where,fields)
				continue
			if kw_id > 0 and status == g_status["has_matched"]:
				logging.info("process mid: %s matched %s" % (id,kw_id))
				###callback
				if callback(srcid,kw_id,task_type,callback_url,0,reason, callback_key) == 200:
					###update status
					fields = {"status":g_status["task_suc"]}
					where = "id=%s" % (id)
					sql_src_class.mysqlUpdate("AutoTask",where,fields)
				else:
					###update status
					fields = {"status":g_status["callback_fail"]}
					where = "id=%s" % (id)
					sql_src_class.mysqlUpdate("AutoTask",where,fields)
				continue
			if taskid <= 0:
				logging.info("process mid: %s failed" % (id))
				###callback
				if callback(srcid,0,task_type,callback_url,1,reason, callback_key) == 200:
					###update status
					fields = {"status":g_status["task_fail"]}
					where = "id=%s" % (id)
					sql_src_class.mysqlUpdate("AutoTask",where,fields)
				else:
					###update status
					fields = {"status":g_status["callback_fail"]}
					where = "id=%s" % (id)
					sql_src_class.mysqlUpdate("AutoTask",where,fields)
				continue
			if not task_type_map.has_key(task_type):
				continue
			kuwo_id,status,reason = get_realId_from_task(taskid, task_type_map[task_type], g_connTask, g_curTask)
			logging.info("process task %s" % (taskid))
			logging.info("table_id: %s status: %s" % (kuwo_id,status))
			if status == "success":
				logging.info("process mid: %s success" % (id))
				###callback
				if callback(srcid,kuwo_id,task_type,callback_url,0,"success", callback_key) == 200:
					###update status
					fields = {"kw_id":kuwo_id,"status":g_status["task_suc"],"reason":""}
					where = "id=%s" % (id)
					sql_src_class.mysqlUpdate("AutoTask",where,fields)
				else:
					###update status
					fields = {"status":g_status["callback_fail"]}
					where = "id=%s" % (id)
					sql_src_class.mysqlUpdate("AutoTask",where,fields)
			elif status == "fail":
				logging.info("process mid: %s fail" % (id))
				###callback
				if callback(srcid,kuwo_id,task_type,callback_url,1,reason, callback_key) == 200:
					###update status
					fields = {"kw_id":kuwo_id,"status":g_status["task_fail"]}
					where = "id=%s" % (id)
					sql_src_class.mysqlUpdate("AutoTask",where,fields)
					logging.info("process mid: %s fail" % (id))
				else:
					###update status
					fields = {"kw_id":kuwo_id,"status":g_status["callback_fail"]}
					where = "id=%s" % (id)
					sql_src_class.mysqlUpdate("AutoTask",where,fields)
			else:
				continue
			#break
		
		g_curSrc.close()
		g_connSrc.close()

		g_curRes.close()
		g_connRes.close()

		g_curRun.close()
		g_connRun.close()

		g_curTask.close()
		g_connTask.close()
	except Exception,e:
		logging.error(str(e))
		traceback.print_exc()
		#os.kill(os.getpid(), signal.SIGTERM)
		pass

if __name__ == "__main__":
	g_mutex = threading.Lock()
	g_connSrc = g_pool_Src.connection()
	g_curSrc = g_connSrc.cursor()
	task_dispatch(g_connSrc, g_curSrc)
	g_curSrc.close()
	g_connSrc.close()
	for i in range(1):
		t = threading.Thread(target=update_task_poll)
		t.start()
	time.sleep(10)

