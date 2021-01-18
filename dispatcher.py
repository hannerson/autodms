# -*- coding=utf-8 -*-
import threading
import Queue
import signal
from pooldb import *
from sqlClass import *
import utils
import traceback
import json
import time
import dmsMatcher
import requests
import random
import string
from task_poll_thread import *

reload(sys)
sys.setdefaultencoding('utf-8')

processed = set()

class dispatcher(object):
	def __init__(self,config,logging,taskurl):
		self.config = config
		self.logging = logging
		self.q_data = Queue.Queue(20000)
		#self.taskprocessor = task
		self.has_data = True
		self.taskurl = taskurl
		self.data_lock = threading.Lock()
		pass

	def check_music_count(self,sql,conn,cur):
		cnt = cur.execute(sql)
		conn.commit()
		if cnt > 0:
			ret = cur.fetchone()
			return ret['count(*)']
		else:
			return 1

	def get_audiosource(self,sig1,sig2,conn,cur):
		try:
			sql = "select id from AudioSource where file_sig1=%s and file_sig2=%s and version_editor>1 limit 1" % (sig1,sig2)
			cnt = cur.execute(sql)
			if cnt > 0:
				ret = cur.fetchone()
				return ret[0]
			return 0
		except Exception as e:
			self.logging.error(str(e))
			return 0

	def get_picsource(self,sig1,sig2,file_type,conn,cur):
		try:
			sql = "select id from PicSource where file_sig1=%s and file_sig2=%s and file_type=\"%s\" and version_editor>1 limit 1" % (sig1,sig2,file_type)
			cnt = cur.execute(sql)
			if cnt > 0:
				ret = cur.fetchone()
				return ret[0]
			return 0
		except Exception as e:
			self.logging.error(str(e))
			return 0

	def get_album_batch_artist(self,aid,conn,cur):
		isbatch = 0
		allartistid = ""
		sql = '''select isbatch,allartistid from Resource.Album where id=%s''' % (aid)
		cnt = cur.execute(sql)
		if cnt > 0:
			ret = cur.fetchone()
			isbatch = ret[0]
			allartistid = ret[1]
		return isbatch,allartistid

	def get_batch(self,aid,t,conn,cur):
		isbatch = 0
		sql = '''select c_batch from DMSRuntime.%s where id=%s''' % (t,aid)
		cnt = cur.execute(sql)
		if cnt > 0:
			ret = cur.fetchone()
			isbatch = ret[0]
		return isbatch

	def get_all_cdns_availible(self,conn,cur):
                server_ip = ""
                server_ips = []
                sql = '''select ip_cdn,weight from AutoDMS.IP_CDN where percent<95 and m_status=0'''
                cnt = cur.execute(sql)
                if cnt > 0:
                        rets = cur.fetchall()
                        conn.commit()
                        for ret in rets:
                                for i in range(0,ret[1]):
                                        server_ips.append(ret[0].strip())
                server_ip = server_ips[random.randint(0,len(server_ips)-1)]
                return server_ip

	def send_task(self,info,connSrc,curSrc,connRun,curRun):
		try:
			#return
			task_id = 0
			pid = info["id"]
			kw_id = info["kw_id"]
			task_type = info["type"]
			src_code = info["src_code"]
			taskinfo = info["taskinfo"]
			ret_taskinfo = {}
			self.logging.info("processing id - %s" % (pid))
			src_sql_class = sqlClass(connSrc,curSrc)
			srcinfo = task_dispatch_single(pid, connSrc, curSrc)
			taskinfo["editor_id"] = "220"
			if taskinfo.has_key("editor_id") and taskinfo["editor_id"] == "":
				taskinfo["editor_id"] = "220"
			elif not taskinfo.has_key("editor_id"):
				taskinfo["editor_id"] = "220"
			callback_key = g_config.configinfo["common"]["callback_key"]

			for k,v in taskinfo.items():
				if k in ["info"]:
					continue
				ret_taskinfo[k] = "%s" % (v)

			ret_taskinfo["info"] = []
			#if kw_id > 0:
			#print taskinfo
			self.logging.info(str(taskinfo))
			#d_info = json.loads(taskinfo)
			no_pic = False
			for i in taskinfo["info"]:
				#print (i)
				#self.logging.info(str(i))
				if i.has_key("Artist"):
					c_batch = 117
					if kw_id > 0:
						i["Artist"]["id"] = "%s" % (kw_id)
						c_batch = self.get_batch(kw_id, "Artist", connRun, curRun)
						if c_batch != 888:
							c_batch = 117
					elif i["Artist"]["id"].isdigit():
						kw_id = int(i["Artist"]["id"])
					if i["Artist"].has_key("m_name"):
						i["Artist"]["m_name"] = "%s" % (i["Artist"]["m_name"].strip())
					if not i["Artist"].has_key("c_show_type"):
						i["Artist"]["c_show_type"] = "%s" % (0)
					else:
						i["Artist"]["c_show_type"] = "%s" % (i["Artist"]["c_show_type"])
					i["Artist"]["c_batch"] = "%s" % (c_batch)
					#break
				elif i.has_key("Music"):
					c_batch = 117
					if kw_id > 0:
						i["Music"]["id"] = "%s" % (kw_id)
						c_batch = self.get_batch(kw_id, "Music", connRun, curRun)
						if c_batch != 888:
							c_batch = 117
					elif i["Music"]["id"].isdigit():
						kw_id = int(i["Music"]["id"])
					if i["Music"].has_key("m_name"):
						i["Music"]["m_name"] = "%s" % (i["Music"]["m_name"].strip())
					if i["Music"].has_key("m_track"):
						i["Music"]["m_track"] = "%s" % (i["Music"]["m_track"])
					if not i["Music"].has_key("c_show_type"):
						i["Music"]["c_show_type"] = "%s" % (0)
					else:
						if str(i["Music"]["c_show_type"]).isdigit() and int(i["Music"]["c_show_type"]) > 0:
							i["Music"]["c_show_type"] = "%s" % (i["Music"]["c_show_type"])
					i["Music"]["c_batch"] = "%s" % (c_batch)
					#break
				elif i.has_key("Album"):
					c_batch = 117
					if kw_id > 0:
						i["Album"]["id"] = "%s" % (kw_id)
						c_batch = self.get_batch(kw_id, "Album", connRun, curRun)
						if c_batch != 888:
							c_batch = 117
					elif i["Album"]["id"].isdigit():
						kw_id = int(i["Album"]["id"])
					if i["Album"].has_key("m_name"):
						i["Album"]["m_name"] = "%s" % (i["Album"]["m_name"].strip())
					if not i["Album"].has_key("c_show_type"):
						i["Album"]["c_show_type"] = "%s" % (0)
					else:
						if str(i["Album"]["c_show_type"]).isdigit() and int(i["Album"]["c_show_type"]) > 0:
							i["Album"]["c_show_type"] = "%s" % (i["Album"]["c_show_type"])
					i["Album"]["c_batch"] = "%s" % (c_batch)
					#break
				elif i.has_key("PicSource"):
					if len(i["PicSource"]) == 0:
						no_pic = True
					print ("----%s--%s" % (no_pic, len(i["PicSource"])))
			#taskinfo = json.dumps(d_info)
			#return
			#pass
			audiosourceid = 0
			picsourceid = 0
			pic_update = True
			audio_update = True
			###process c-exparams
			for i in taskinfo["info"]:
				if i.has_key("AudioSource"):
					#i["AudioSource"]["c_extparams"] = "orig_ip=%s&targ_ip=%s" % (self.config.configinfo["common"]["orig_ip"],self.config.configinfo["common"]["targ_ip"])
					if not i["AudioSource"].has_key("file_format") and i["AudioSource"].has_key("file_path") and i["AudioSource"]["file_path"] != "":
						i["AudioSource"]["file_format"] = i["AudioSource"]["file_path"].strip().split(".")[-1]
					elif i["AudioSource"].has_key("file_path") and i["AudioSource"]["file_path"] == "":
						field_dict = {}
						field_dict["status"] = g_status["wrong_info"]
						field_dict["taskid"] = task_id
						field_dict["reason"] = "%s" % ("no filepath")
						where = "id=%s" % (pid)
						cnt = src_sql_class.mysqlUpdate("AutoTask",where,field_dict)
						callback(srcinfo["srcid"],srcinfo["kw_id"],srcinfo["type"],srcinfo["callback_url"],1,"no filepath", callback_key)
						return
					if kw_id == 0 and (not i["AudioSource"].has_key("file_sig1") or not i["AudioSource"].has_key("file_sig2") or not i["AudioSource"].has_key("file_path")):
						field_dict = {}
						field_dict["status"] = g_status["wrong_info"]
						field_dict["taskid"] = task_id
						field_dict["reason"] = "%s" % ("no sig")
						where = "id=%s" % (pid)
						cnt = src_sql_class.mysqlUpdate("AutoTask",where,field_dict)
						callback(srcinfo["srcid"],srcinfo["kw_id"],srcinfo["type"],srcinfo["callback_url"],1,"no sig", callback_key)
						return
					if i["AudioSource"].has_key("file_sig1") and i["AudioSource"].has_key("file_sig2"):
						if i["AudioSource"]["file_sig1"] == "" or i["AudioSource"]["file_sig2"] == "":
							field_dict = {}
							field_dict["status"] = g_status["wrong_info"]
							field_dict["taskid"] = task_id
							field_dict["reason"] = "%s" % ("no sig")
							where = "id=%s" % (pid)
							cnt = src_sql_class.mysqlUpdate("AutoTask",where,field_dict)
							callback(srcinfo["srcid"],srcinfo["kw_id"],srcinfo["type"],srcinfo["callback_url"],1,"no sig", callback_key)
							return
						audiosourceid = self.get_audiosource(i["AudioSource"]["file_sig1"],i["AudioSource"]["file_sig2"],connRun,curRun)
					if audiosourceid > 0:
						#ret_taskinfo["info"].append(i)
						continue
				if i.has_key("PicSource") and no_pic == False:
					i["PicSource"]["c_extparams"] = "nozcopy=1&scale=1"
					if task_type == 2:
						i["PicSource"]["file_type"] = "pic_bat_album"
					elif task_type == 3:
						i["PicSource"]["file_type"] = "pic_bat_artist"
					if i["PicSource"].has_key("file_sig1") and i["PicSource"].has_key("file_sig2") and i["PicSource"]["file_sig1"] !="" and i["PicSource"]["file_sig2"] !="":
						picsourceid = self.get_picsource(i["PicSource"]["file_sig1"],i["PicSource"]["file_sig2"],i["PicSource"]["file_type"],connRun,curRun)
					elif kw_id > 0:
						del i["PicSource"]
						pic_update = False
					else:
						field_dict = {}
						field_dict["status"] = g_status["wrong_info"]
						field_dict["taskid"] = task_id
						field_dict["reason"] = "%s" % ("no sig")
						where = "id=%s" % (pid)
						cnt = src_sql_class.mysqlUpdate("AutoTask",where,field_dict)
						callback(srcinfo["srcid"],srcinfo["kw_id"],srcinfo["type"],srcinfo["callback_url"],1,"no sig", callback_key)
						return
					if picsourceid > 0:
						#ret_taskinfo["info"].append(i)
						continue
			print (taskinfo)
			print ("--------------")
			for i in taskinfo["info"]:
				print (i)
				if len(i) == 0:
					#print ("----0000000000000000----------")
					continue
				if i.has_key("AudioSource") and audiosourceid > 0:
					continue
				if i.has_key("PicSource") and (picsourceid > 0 or no_pic):
					print ("----0000000000000000----------")
					continue
				if i.has_key("Artist"):
				       	#i["Artist"]["c_batch"] = "%s" % (117)
					if i["Artist"].has_key("m_name"):
						i["Artist"]["m_name"] = "%s" % (i["Artist"]["m_name"].strip())
					if not i["Artist"].has_key("c_show_type"):
						i["Artist"]["c_show_type"] = "%s" % (0)
				        else:
						i["Artist"]["c_show_type"] = "%s" % (i["Artist"]["c_show_type"])
					if pic_update == False or no_pic:
						del i["Artist"]["m_pic_id"]
					if picsourceid > 0:
						i["Artist"]["m_pic_id"] = "%s" % (picsourceid)
				if i.has_key("Album"):
				       #i["Album"]["c_batch"] = "%s" % (117)
				       if not i["Album"].has_key("c_show_type"):
						i["Album"]["c_show_type"] = "%s" % (0)
				       else:
						i["Album"]["c_show_type"] = "%s" % (i["Album"]["c_show_type"])
				       if pic_update == False or no_pic:
						del i["Album"]["m_pic_id"]
				       if picsourceid > 0:
						i["Album"]["m_pic_id"] = "%s" % (picsourceid)
				if i.has_key("Music"):
				       	#i["Music"]["c_batch"] = "%s" % (117)
					if i["Music"].has_key("m_name"):
						i["Music"]["m_name"] = "%s" % (i["Music"]["m_name"].strip())
					if i["Music"].has_key("m_track"):
						if src_code == 2:
							i["Music"]["m_track"] = "%s" % (i["Music"]["m_track"])
						elif src_code == 1:
							i["Music"].pop("m_track")
					if audio_update == False:
						del i["Music"]["m_audio_id"]
					if audiosourceid > 0:
						i["Music"]["m_audio_id"] = "%s" % (audiosourceid)
					if not i["Music"].has_key("c_show_type"):
						i["Music"]["c_show_type"] = "%s" % (0)
					elif i["Music"].has_key("c_show_type"):
						i["Music"]["c_show_type"] = "%s" % (i["Music"]["c_show_type"])
				print ("--------------")
				print (i)
				ret_taskinfo["info"].append(i)
			ret_taskinfo["count"] = "%s" % (len(ret_taskinfo["info"]))
			task_id,reason = self.sendTask(ret_taskinfo)
			#src_sql_class = sqlClass(connSrc,curSrc)
			if task_id > 0:
				field_dict = {}
				field_dict["status"] = g_status["task_send"]
				field_dict["taskid"] = task_id
				field_dict["reason"] = ""
				where = "id=%s" % (pid)
				cnt = src_sql_class.mysqlUpdate("AutoTask",where,field_dict)
				if cnt == 0:
					self.logging.info("update task id-%s taskid-%s m_status-%s failed" % (id,task_id,"task_send"))
			elif task_id == -1:
				field_dict = {}
				field_dict["status"] = g_status["wrong_info"]
				field_dict["taskid"] = task_id
				field_dict["reason"] = "%s" % (reason)
				where = "id=%s" % (pid)
				cnt = src_sql_class.mysqlUpdate("AutoTask",where,field_dict)
				callback(srcinfo["srcid"],srcinfo["kw_id"],srcinfo["type"],srcinfo["callback_url"],1,reason, callback_key)
				if cnt == 0:
					self.logging.info("update task id-%s taskid-%s m_status-%s failed" % (id,task_id,"task_send"))
			else:
				#time.sleep(1)
				self.logging.info("something error or no task")
		except Exception,e:
			self.logging.error(str(e))
			#self.data_lock.acquire()
			traceback.print_exc()
			self.has_data = False
			self.logging.error("error has data : %s" % (self.has_data))
			os.kill(os.getpid(), signal.SIGTERM)
			#self.data_lock.release()
			pass

	def sendTask(self, info):
		try:
			task_id = 0
			reason = ""
			#task_id = 30884833 #Music Album
			#task_id = 30916210 #Artist
			if len(info) == 0:
				return task_id
			post_json = json.dumps(info)
			self.logging.info(str(post_json))
			#'''
			#f = urllib2.urlopen(
			#	url     = self.taskurl,
			#	data    = post_json
			#	)
			headers = {"Content-Type":"application/json;charset:utf-8;"}
			f = requests.post(
				url = self.taskurl,
				headers = headers,
				data = post_json
			)
			result = f.text
			self.logging.info(str(result))
			js_ret = json.loads(result)
			if js_ret.has_key("taskid") and int(js_ret["taskid"]) > 0:
				task_id = int(js_ret["taskid"])
			elif js_ret.has_key("ret") and js_ret["ret"] != 0:
				task_id = -1
				reason = js_ret["msg"]
			elif js_ret.has_key("code") and js_ret["code"] !=0 and js_ret.has_key("message"):
				task_id = -1
				reason = js_ret["message"]
			#'''
			return task_id,reason
		except Exception,e:
			self.logging.info(str(e))
			traceback.print_exc()
			return 0,""

	def get_keys_in_task(self,type,tableSrc,conn,cur):
		try:
			if type in ["Artist","Video"]:
				return set()
			keys = set()
			sql = '''select m_name,m_artists from %s where m_status=4''' % (tableSrc)
			#sql = '''select m_name,m_artists from %s where tx_albumid="10001522"''' % (tableSrc)
			self.logging.info(sql)
			cnt = cur.execute(sql)
			conn.commit()
			if cnt < 1:
				self.logging.info("no data in task")
			rets = cur.fetchall()
			for ret in rets:
				m_name = ret["m_name"].strip()
				arts = ret["m_artists"].strip().split("###")
				arts.sort()
				m_artists = "###".join(arts)
				k = "%s-%s" % (m_name,m_artists)
				keys.add(k)
			return keys
		except Exception,e:
			self.logging.error(str(e))
			self.has_data = False
			return set()
			pass

	def thread_worker(self,i):
		try:
			while self.has_data :
				self.logging.info("g_has_data : %s" % (self.has_data))
				taskinfo = {}
				if self.q_data.empty():
					time.sleep(1)
					self.logging.info("sleep 1s")
				else:
					self.data_lock.acquire()
					if not self.q_data.empty():
						taskinfo = self.q_data.get()
						self.data_lock.release()
					else:
						self.data_lock.release()
					self.logging.info("-------worker %s---------" % (i))
					if len(taskinfo) == 0:
						continue
					connSrc = g_pool_Src.connection()
					curSrc = connSrc.cursor()
					connRun = g_pool_Run.connection()
					curRun = connRun.cursor()

					self.send_task(taskinfo,connSrc,curSrc,connRun,curRun)

					curRun.close()
					connRun.close()
					curSrc.close()
					connSrc.close()
		except Exception,e:
			self.logging.error(str(e))
			self.has_data = False
			pass

	def get_data(self,conn,cur,limit):
		config_info = self.config.configinfo

		tsk_status = set()
		tsk_status.add(g_status["default"])
		#tsk_status.add(g_status["task_fail"])

		sql_status = ""
		for s in tsk_status:
			sql_status += "%s," % (s)
			
		sql_fields = "id,content,kw_id,type,src_code"
		sql = '''select %s from %s where status in (%s) order by priority desc limit %s''' % (sql_fields.rstrip(","), "AutoTask", sql_status.rstrip(","), limit)
		self.logging.info(sql)
		cnt = cur.execute(sql)
		conn.commit()
		if cnt < 1:
			self.logging.info("no data need to dispatch")
		
		ret_sql = cur.fetchall()
		###repetition process
		src_sql_class = sqlClass(conn,cur)
		ret_sql_process = []
		callback_key = g_config.configinfo["common"]["callback_key"]
		global processed
		for result in ret_sql:
			###1.parse the task content
			content = result["content"]
			pid = result["id"]
			kw_id = result["kw_id"]
			task_type = result["type"]
			src_code = result["src_code"]
			task_info = json.loads(content)
			srcinfo = task_dispatch_single(pid, conn, cur)
			self.logging.info("process %s" % (pid))
			if pid in processed:
				self.logging.info("process %s has processed" % (pid))
				continue

			processed.add(pid)

			###2.check normal
			if not task_info.has_key("info") or not task_info.has_key("count") or not task_info.has_key("priority") or not task_info.has_key("editor_id"):
				if callback(srcinfo["srcid"],srcinfo["kw_id"],srcinfo["type"],srcinfo["callback_url"],1,"task info not complete", callback_key) == 200:
					params = {}
					params["status"] = g_status["retry_fail"]
					params["reason"] = "task info not complete: parameter info or priority"
					where = "id=%s" % (pid)
					src_sql_class.mysqlUpdate("AutoTask",where,params)
				else:
					params = {}
					params["status"] = g_status["retry_fail"]
					params["reason"] = "callback failed"
					where = "id=%s" % (pid)
					src_sql_class.mysqlUpdate("AutoTask",where,params)
				continue
			###3.check repetition
			check_info = True
			infos = task_info["info"]
			for info in infos:
				for k,v in info.items():
					if not v.has_key("id"):
						if callback(srcinfo["srcid"],srcinfo["kw_id"],srcinfo["type"],srcinfo["callback_url"],1,"task info not complete", callback_key) == 200:
							params = {}
							params["status"] = g_status["retry_fail"]
							params["reason"] = "task info not complete"
							where = "id=%s" % (pid)
							src_sql_class.mysqlUpdate("AutoTask",where,params)
						else:
							params = {}
							params["status"] = g_status["retry_fail"]
							params["reason"] = "task info not complete"
							where = "id=%s" % (pid)
							src_sql_class.mysqlUpdate("AutoTask",where,params)
						check_info = False
						break
				if check_info == False:
					break

				###4.send task
				if check_info:
					if info.has_key("PicSource"):
						element = info["PicSource"]
						if not element.has_key("file_format") or not element.has_key("file_path"):
							info["PicSource"] = {}
							#if callback(srcinfo["srcid"],srcinfo["kw_id"],srcinfo["type"],srcinfo["callback_url"],1,"picsource info not complete",callback_key) == 200:
							#	params = {}
							#	params["status"] = g_status["retry_fail"]
							#	params["reason"] = "task info not complete"
							#	where = "id=%s" % (pid)
							#	src_sql_class.mysqlUpdate("AutoTask",where,params)
							#check_info = False
							#break
						pre_local_dir = g_config.configinfo["common"]["pre_local_dir"]
						###1 download
						tmp_local_dir = "pic/%s_%s" % ("pic",''.join(random.sample(string.ascii_letters + string.digits, 8)))
						ret = utils.download_url(pre_local_dir + tmp_local_dir,element["file_path"])
						if ret != 0:
							#if callback(srcinfo["srcid"],srcinfo["kw_id"],srcinfo["type"],srcinfo["callback_url"],1,"pic download failed", callback_key) == 200:
							#	params = {}
							#	params["status"] = g_status["retry_fail"]
							#	params["reason"] = "pic download failed"
							#	where = "id=%s" % (pid)
							#	src_sql_class.mysqlUpdate("AutoTask",where,params)
							#check_info = False
							info["PicSource"] = {}
						else:
							###2 mksig
							sig1,sig2 = utils.mksig(pre_local_dir + tmp_local_dir)
							filesize = os.path.getsize(pre_local_dir + tmp_local_dir)
							element["file_size"] = "%s" % (filesize)
							element["file_sig1"] = "%s" % (sig1)
							element["file_sig2"] = "%s" % (sig2)
							element["c_extparams"] = ("nozcopy=1&scale=1")
							element["file_path"] = "%s%s" % (g_config.configinfo["common"]["pre_http"],tmp_local_dir)
							if info.has_key("Artist"):
								element["file_type"] = "pic_bat_artist"
							if info.has_key("Album"):
								element["file_type"] = "pic_bat_album"
					if info.has_key("AudioSource"):
						element = info["AudioSource"]
						if not element.has_key("file_format") or not element.has_key("file_path"):
							if callback(srcinfo["srcid"],srcinfo["kw_id"],srcinfo["type"],srcinfo["callback_url"],1,"audiosource info not complete", callback_key) == 200:
								params = {}
								params["status"] = g_status["retry_fail"]
								params["reason"] = "task info not complete"
								where = "id=%s" % (pid)
								src_sql_class.mysqlUpdate("AutoTask",where,params)
							check_info = False
							break
						pre_local_dir = g_config.configinfo["common"]["pre_local_dir"]
						###1 download
						tmp_local_dir = "audio/%s_%s.%s" % ("audio",''.join(random.sample(string.ascii_letters + string.digits, 8)),element["file_format"])
						ret = utils.download_url(pre_local_dir + tmp_local_dir,element["file_path"])
						if ret != 0:
							if callback(srcinfo["srcid"],srcinfo["kw_id"],srcinfo["type"],srcinfo["callback_url"],1,"audio download failed", callback_key) == 200:
								params = {}
								params["status"] = g_status["retry_fail"]
								params["reason"] = "audio download failed"
								where = "id=%s" % (pid)
								src_sql_class.mysqlUpdate("AutoTask",where,params)
							check_info = False
							break
						###2 mksig
						g_connAuto = g_pool_Auto.connection()
						g_curAuto = g_connAuto.cursor()
						targ_ip = self.get_all_cdns_availible(g_connAuto,g_curAuto)
						g_curAuto.close()
						g_connAuto.close()
						sig1,sig2 = utils.mksig(pre_local_dir + tmp_local_dir)
						element["file_sig1"] = "%s" % (sig1)
						element["file_sig2"] = "%s" % (sig2)
						element["c_extparams"] = "orig_ip=%s&targ_ip=%s" % (self.config.configinfo["common"]["orig_ip"],targ_ip)
						element["file_path"] = "%s" % (utils.songFileProcess(sig1,sig2,g_config.configinfo["common"]["pre_local_dir"]+"/audio/", pre_local_dir+tmp_local_dir))
						element["file_type"] = "audio_mass"
					if info.has_key("Artist"):
						element = info["Artist"]
						if element.has_key("id") and str(element["id"]).isdigit():
							###check id with record id
							if int(element["id"]) > 0 and kw_id >0 and int(element["id"]) != kw_id:
								if callback(srcinfo["srcid"],srcinfo["kw_id"],srcinfo["type"],srcinfo["callback_url"],1,"task info not complete", callback_key) == 200:
									params = {}
									params["status"] = g_status["retry_fail"]
									params["reason"] = "id not match in task"
									where = "id=%s" % (pid)
									src_sql_class.mysqlUpdate("AutoTask",where,params)
								check_info = False
								break
						elif element.has_key("id") and element["id"] == "NEW_Artist":
							if not element.has_key("m_name"):
								if callback(srcinfo["srcid"],srcinfo["kw_id"],srcinfo["type"],srcinfo["callback_url"],1,"task info not complete", callback_key) == 200:
									params = {}
									params["status"] = g_status["retry_fail"]
									params["reason"] = "task info not complete"
									where = "id=%s" % (pid)
									src_sql_class.mysqlUpdate("AutoTask",where,params)
								check_info = False
								break

							###check repeat
							artist = element["m_name"].strip()
							artist_loginid = 0
							artist_id = 0
							if element.has_key("artist_loginid"):
								artist_loginid = int(element["artist_loginid"])
							if artist_loginid > 0:
								connRun = g_pool_Run.connection()
								curRun = connRun.cursor()
								artist_id = dmsMatcher.getArtistIdFromName(artist,artist_loginid,connRun,curRun)
								curRun.close()
								connRun.close()
							if artist_id > 0:
								if callback(srcinfo["srcid"],artist_id,srcinfo["type"],srcinfo["callback_url"],0,"", callback_key) == 200:
									params = {}
									params["status"] = g_status["has_matched"]
									params["reason"] = "matched in database"
									params["kw_id"] = "%s" % (artist_id)
									where = "id=%s" % (pid)
									src_sql_class.mysqlUpdate("AutoTask",where,params)
									check_info = False
									break
					elif info.has_key("Album"):
						element = info["Album"]
						if element.has_key("id") and str(element["id"]).isdigit():
							###check id with record id
							if int(element["id"]) > 0 and kw_id > 0 and int(element["id"]) != kw_id:
								if callback(srcinfo["srcid"],srcinfo["kw_id"],srcinfo["type"],srcinfo["callback_url"],1,"id not match in task", callback_key) == 200:
									params = {}
									params["status"] = g_status["retry_fail"]
									params["reason"] = "id not match in task"
									where = "id=%s" % (pid)
									src_sql_class.mysqlUpdate("AutoTask",where,params)
								check_info = False
								break
						elif element.has_key("id") and element["id"] == "NEW_Album":
							if not element.has_key("m_name") or not element.has_key("m_artists"):
								if callback(srcinfo["srcid"],srcinfo["kw_id"],srcinfo["type"],srcinfo["callback_url"],1,"task info not complete", callback_key) == 200:
									params = {}
									params["status"] = g_status["retry_fail"]
									params["reason"] = "task info not complete"
									where = "id=%s" % (pid)
									src_sql_class.mysqlUpdate("AutoTask",where,params)
								check_info = False
								break
							###check repeat
							artistids = []
							for k in element["m_artists"]:
								if str(k).isdigit():
									artistids.append(int(k))
								else:
									params = {}
									params["reason"] = "task info artist id wrong"
									where = "id=%s" % (pid)
									if callback(srcinfo["srcid"],srcinfo["kw_id"],srcinfo["type"],srcinfo["callback_url"],1,"task info artist id wrong", callback_key) == 200:
										params["status"] = g_status["retry_fail"]
									else:
										params["status"] = g_status["callback_fail"]
									src_sql_class.mysqlUpdate("AutoTask",where,params)
									check_info = False
									break
							if check_info == False:
								break
							if kw_id == 0:
								connRun = g_pool_Run.connection()
								curRun = connRun.cursor()
								albumids = dmsMatcher.matchAlbumSql(element["m_name"].strip(),artistids,connRun,curRun)
								curRun.close()
								connRun.close()
								if len(albumids) > 0:
									params = {}
									params["status"] = g_status["has_matched"]
									params["reason"] = "matched in database"
									params["kw_id"] = "%s" % (albumids[0])
									where = "id=%s" % (pid)
									logging.info("matched in dms %s - %s" % (pid,albumids[0]))
									src_sql_class.mysqlUpdate("AutoTask",where,params)
									check_info = False
									break
							###TODO:parse payinfo
							if element.has_key("payInfo"):
								payinfo = element["payInfo"]
								del element["payInfo"]
								params = {}
								params["payinfo"] = json.dumps(payinfo)
								where = "id=%s" % (pid)
								src_sql_class.mysqlUpdate("AutoTask",where,params)
								
					elif info.has_key("Music"):
						element = info["Music"]
						if element.has_key("id") and str(element["id"]).isdigit():
							###check id with record id
							if int(element["id"]) > 0 and kw_id > 0 and int(element["id"]) != kw_id:
								if callback(srcinfo["srcid"],srcinfo["kw_id"],srcinfo["type"],srcinfo["callback_url"],1,"id not match in task", callback_key) == 200:
									params = {}
									params["status"] = g_status["retry_fail"]
									params["reason"] = "id not match in task"
									where = "id=%s" % (pid)
									logging.info("id not match in task - %s" % (pid))
									src_sql_class.mysqlUpdate("AutoTask",where,params)
								check_info = False
								break
						elif element.has_key("id") and element["id"] == "NEW_Music":
							if not element.has_key("m_name") or not element.has_key("m_artists"):
								if callback(srcinfo["srcid"],srcinfo["kw_id"],srcinfo["type"],srcinfo["callback_url"],1,"task info not complete", callback_key) == 200:
									params = {}
									params["status"] = g_status["retry_fail"]
									params["reason"] = "task info not complete"
									where = "id=%s" % (pid)
									logging.info("task info artist id wrong - %s" % (pid))
								src_sql_class.mysqlUpdate("AutoTask",where,params)
								check_info = False
								break
							###check repeat
							version = ""
							albumid = 0
							if element.has_key("basic_version"):
								version = element["basic_version"].strip()
							if element.has_key("m_album_id"):
								if element["m_album_id"].isdigit():
									albumid = int(element["m_album_id"])
								else:
									if callback(srcinfo["srcid"],srcinfo["kw_id"],srcinfo["type"],srcinfo["callback_url"],1,"task info album id wrong", callback_key) == 200:
										params = {}
										params["status"] = g_status["retry_fail"]
										params["reason"] = "task info album id wrong"
										where = "id=%s" % (pid)
										logging.info("task info album id wrong - %s" % (pid))
										src_sql_class.mysqlUpdate("AutoTask",where,params)
									else:
										params = {}
										params["status"] = g_status["retry_fail"]
										params["reason"] = "task info album id wrong"
										where = "id=%s" % (pid)
										logging.info("task info album id wrong - %s" % (pid))
										src_sql_class.mysqlUpdate("AutoTask",where,params)
									check_info = False
									break
							artistids = []
							for k in element["m_artists"]:
								if str(k).isdigit():
									artistids.append(int(k))
								else:
									params = {}
									params["reason"] = "task info artist id wrong"
									where = "id=%s" % (pid)
									logging.info("task info artist id wrong - %s" % (pid))
									if callback(srcinfo["srcid"],srcinfo["kw_id"],srcinfo["type"],srcinfo["callback_url"],1,"task info artist id wrong", callback_key) == 200:
										params["status"] = g_status["retry_fail"]
									else:
										params["status"] = g_status["callback_fail"]
									src_sql_class.mysqlUpdate("AutoTask",where,params)
									check_info = False
									break
							if check_info == False:
								break
							logging.info("match music %s - %s - %s" % (element["m_name"].strip(),albumid,str(artistids)))
							connRun = g_pool_Run.connection()
							curRun = connRun.cursor()
							albumids = dmsMatcher.matchMusicSql(element["m_name"].strip(),version,"",albumid,artistids,connRun,curRun)
							curRun.close()
							connRun.close()
							if len(albumids) > 0:
								params = {}
								params["status"] = g_status["has_matched"]
								params["reason"] = "matched in database"
								params["kw_id"] = "%s" % (albumids[0])
								where = "id=%s" % (pid)
								logging.info("matched in dms %s - %s" % (pid,albumids[0]))
								src_sql_class.mysqlUpdate("AutoTask",where,params)
								check_info = False
								break
							###TODO:parse payinfo
							if element.has_key("payInfo"):
								payinfo = element["payInfo"]
								del element["payInfo"]
								params = {}
								params["payinfo"] = json.dumps(payinfo)
								where = "id=%s" % (pid)
								src_sql_class.mysqlUpdate("AutoTask",where,params)
					else:
						continue
			logging.info(task_info)
			if check_info:
				logging.info("put id - %s" % (pid))
				self.data_lock.acquire()
				self.q_data.put({"id":pid,"kw_id":kw_id,"taskinfo":task_info,"type":task_type,"src_code":src_code})
				self.data_lock.release()

	def create_workers(self):
		config_info = self.config.configinfo
		thread_num = 3
		if config_info.has_key("common") and config_info["common"].has_key("thread_num"):
			thread_num = int(config_info["common"]["thread_num"])
		for i in range(thread_num):
			t1 = threading.Thread(target=self.thread_worker, args=(i,))
			t1.start()

	def dispatch(self,limit,tasklimit,pool_Src,pool_Run,pool_Res):
		try:
			connSrc = pool_Src.connection()
			curSrc = connSrc.cursor()
			connRun = pool_Run.connection()
			curRun = connRun.cursor()
			connTask = g_pool_Task.connection()
			curTask = connTask.cursor()
			editor_id = self.config.configinfo["common"]["editor_id"]
			task_sum = 0
			count_sql = 'select count(*) from %s where status=0' % "AutoTask"
			count = self.check_music_count(count_sql,connSrc,curSrc)
			#while True:
			print count
			while count > 0:
				self.get_data(connSrc,curSrc,limit)
				#while self.has_data and self.q_data.full():
				while self.has_data and not self.q_data.empty():
					self.logging.info("sleep 2s")
					time.sleep(2)
				#time.sleep(50)
				task_sum += 1
				if task_sum % 10 == 0:
					task_count = utils.get_num_task_running(editor_id, connTask, curTask)
					while task_count > tasklimit:
						self.logging.info("task count %s sleep 10s" % (task_count))
						time.sleep(10)
						task_count = utils.get_num_task_running(editor_id, connTask, curTask)
				count = self.check_music_count(count_sql,connSrc,curSrc)
				self.logging.info("count %s" % (count))
				if self.has_data == False:
					self.logging.info("error exit")
					break
				break
				if count == 0:
					self.logging.info("sleep 60s")
					time.sleep(60)
				#break
			self.has_data = False
			curSrc.close()
			connSrc.close()
			curRun.close()
			connRun.close()
			curTask.close()
			connTask.close()

			self.logging.info("no match task")
		except Exception,e:
			self.logging.error(str(e))
			traceback.print_exc()
			self.has_data = False
			pass

