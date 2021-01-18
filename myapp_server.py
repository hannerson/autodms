from flask import Flask,request,jsonify,json
import json
from pooldb import *
import hashlib
import traceback
from sqlClass import *
import logging
from logging.handlers import TimedRotatingFileHandler
import re
import datetime

app = Flask(__name__)

def initlog():
        logger = logging.getLogger()
        LOG_FILE = "log/" + sys.argv[0].split("/")[-1].replace(".py","") + '.log'
        hdlr = TimedRotatingFileHandler(LOG_FILE,when='D',backupCount=24)
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(filename)s[line:%(lineno)d]:%(message)s')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
        logger.setLevel(logging.NOTSET)
        return logger

logging = initlog()

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
		hl.update(str("%s%s" % (timestamp,private_key)).encode(encoding='utf-8'))
		if digest == hl.hexdigest():
			#print "123"
			return True
		else:
			return False
	except Exception,e:
		traceback.print_exc()
		return False

def insert_database(json_data,dmscode,srcid,callback,tasktype):
	try:
		taskid = -1
		conn = g_pool_Src.connection()
		cur = conn.cursor()
		sql_class = sqlClass(conn,cur)
		#sql = '''insert into AutoTask (src_code,srcid,callback,content) values (%s,%s,"%s","%s")''' % (dmscode,srcid,callback,MySQLdb.escape_string(json_data))
		param = {}
		param["src_code"] = "%s" % (dmscode)
		param["srcid"] = "%s" % (srcid)
		param["callback_url"] = "%s" % (callback)
		param["type"] = "%s" % (tasktype)
		param["content"] = "%s" % (json_data)
		param["status"] = "%s" % (0)
		param["createtime"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
		taskid = sql_class.mysqlInsertUpdate("AutoTask",param,["src_code","srcid","type"])
		cur.close()
		conn.close()
		return taskid
	except Exception,e:
		logging.info(str(e))
		traceback.print_exc()
		return -1

def check_database_status(dmscode,srcid,tasktype):
	try:
		taskid = -1
		kw_id = 0
		status = -1
		reason = ""
		conn = g_pool_Src.connection()
		cur = conn.cursor()
		where = '''src_code=%s and srcid="%s" and type=%s''' % (dmscode,srcid,tasktype)
		sql = '''select id,kw_id,status,reason from AutoTask where %s''' % (where)
		#rets = sql_class.mysqlSelect("AutoTask",where,0,["id","kw_id"])
		#print rets
		cnt = cur.execute(sql)
		if cnt > 0:
			ret = cur.fetchone()
			taskid = ret["id"]
			kw_id = ret["kw_id"]
			status = ret["status"]
			reason = ret["reason"]
		cur.close()
		conn.close()
		return taskid,kw_id,status,reason
	except Exception,e:
		logging.info(str(e))
		traceback.print_exc()
		return -1,0,-1,""

def check_database(dmscode,srcid,tasktype):
	try:
		taskid = -1
		kw_id = 0
		conn = g_pool_Src.connection()
		cur = conn.cursor()
		where = '''src_code=%s and srcid="%s" and type=%s''' % (dmscode,srcid,tasktype)
		sql = '''select id,kw_id from AutoTask where %s''' % (where)
		cnt = cur.execute(sql)
		if cnt > 0:
			ret = cur.fetchone()
			taskid = ret["id"]
			kw_id = ret["kw_id"]
		cur.close()
		cur.close()
		conn.close()
		return taskid,kw_id
	except Exception,e:
		logging.info(str(e))
		traceback.print_exc()

@app.route('/autodms',methods=['POST','GET'])
def unibang():
	try:
		if request.method == 'POST':
			args = request.args
			if args.get("m") != "task":
				return json.dumps({"code":100,"message":"wrong param:m"})
			if args.get("a") not in ["create","query"]:
				return json.dumps({"code":100,"message":"wrong param:a"})
			#print "------------data"
			#print request.data
			#print "-----------values"
			#print request.values
			#print "-----------form"
			#print request.form
			data = {}
			json_data = {}
			#if request.data == "":
			#	return jsonify(code=2003,status=0,message="wrong param:data")
			#logging.info(request.data)
			#print request.data
			#print request.headers
			#print request.get_json()
			data = request.get_json()
			if data is None and request.data != "":
				data = json.loads(request.data)
			print data
			logging.info(str(data))
			if not (data.has_key("client_time") and data.has_key("check_sum")):
				return json.dumps({"code":100,"message":"wrong param"})
			if not checkValid(data['client_time'],data['check_sum']):
				return json.dumps({"code":300,"message":"valid fail"})
			###1.get data json
			if not data.has_key("data"):
				return json.dumps({"code":100,"message":"wrong param:data"})
			if not data.has_key("srcid") or not data.has_key("dmscode") or not data.has_key("callback") or not data.has_key("tasktype"):
				return json.dumps({"code":100,"message":"param:callback-srcid-dmscode-tasktype is needed"})
			if type(data["data"]) == str or type(data["data"]) == unicode:
				print data["data"]
				data["data"]=re.sub('\'','\"',data["data"])
				json_data = json.loads(data["data"])
			elif type(data["data"]) == dict:
				json_data = data["data"]
			json_str = json.dumps(json_data)
			###2.1 check info in database
			taskid, kw_id, status, reason = check_database_status(data["dmscode"],data["srcid"],data["tasktype"])
			if taskid > 0 and status in [g_status["task_send"],g_status["default"]]:
				return json.dumps({"code":300,"message":"task is processing","last_taskid":taskid,"kw_id":kw_id})
			###2.2 insert into database
			taskid = insert_database(json_str,data["dmscode"],data["srcid"],data["callback"],data["tasktype"])
			#if taskid == 0:
			#	taskid, kw_id = check_database(data["dmscode"],data["srcid"],data["tasktype"])
			print taskid
			
			###3.return
			if taskid > 0:
				return json.dumps({"code":0,"message":"success","taskid":taskid})
			elif taskid == 0:
				return json.dumps({"code":100,"message":"wrong param:data"})
			else:
				return json.dumps({"code":100,"message":"wrong param:data"})
		else:
			return json.dumps({"code":100,"message":"wrong method"})
	except Exception,e:
		logging.info(str(e))
		traceback.print_exc()
		return json.dumps({"code":500,"message":"server internal error"})

@app.route('/autodms/query', methods=['POST','GET'])
def autodms_query():
	try:
		if request.method == 'POST':
			#print "------------data"
			#print request.data
			#print "-----------values"
			#print request.values
			#print "-----------form"
			#print request.form
			data = {}
			json_data = {}
			#if request.data == "":
			#	return jsonify(code=2003,status=0,message="wrong param:data")
			#logging.info(request.data)
			#print request.data
			#print request.headers
			#print request.get_json()
			data = request.get_json()
			if data is None and request.data != "":
				data = json.loads(request.data)
			print data
			logging.info(str(data))
			if not (data.has_key("client_time") and data.has_key("check_sum")):
				return json.dumps({"code":100,"message":"wrong param"})
			if not checkValid(data['client_time'],data['check_sum']):
				return json.dumps({"code":300,"message":"valid fail"})
			###1.get data json
			if not data.has_key("srcid") or not data.has_key("dmscode") or not data.has_key("tasktype"):
				return json.dumps({"code":100,"message":"param:srcid-dmscode-tasktype is needed"})
			###2.1 check info in database
			taskid, kw_id, status, reason = check_database_status(data["dmscode"],data["srcid"],data["tasktype"])
			if taskid == -1 and status == -1:
				return json.dumps({"code":0,"message":"has no srcid-%s info" % (data["srcid"]),"taskid":taskid,"kw_id":kw_id, "reason":reason})
			elif taskid > 0 and status in [g_status["task_send"],g_status["default"]]:
				return json.dumps({"code":0,"message":"task is processing","taskid":taskid,"kw_id":kw_id, "reason":reason})
			###3.return
			elif status in [g_status["has_matched"], g_status["task_suc"]]:
				return json.dumps({"code":0, "message":"task success", "taskid":taskid, "kw_id":kw_id, "reason":reason})
			elif status in [g_status["task_fail"], g_status["retry_fail"]]:
				return json.dumps({"code":0, "message":"task fail", "taskid":taskid, "kw_id":kw_id, "reason":reason})
			elif status in [g_status["callback_fail"]]:
				return json.dumps({"code":0, "message":"task success,but callback fail", "taskid":taskid, "kw_id":kw_id, "reason":reason})
			else:
				return json.dumps({"code":0, "message":"unknown error", "taskid":taskid, "kw_id":kw_id, "reason":reason})
		else:
			return json.dumps({"code":100,"message":"wrong method"})
	except Exception,e:
		logging.info(str(e))
		traceback.print_exc()
		return json.dumps({"code":500,"message":"server internal error"})

if __name__ == '__main__':
	app.debug = True
	app.run(host='0.0.0.0',port=6020,threaded=True)
