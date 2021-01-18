#!/bin/python
# -*- coding:utf-8 -*-

import os,sys
import MySQLdb
import logging
import json
import traceback
import urllib2
import random
import string
from PIL import Image
import datetime
import time
from sqlClass import *
from utils import *
from logger import *
from pooldb import *
from resource_request import *
import requests
import re
from lrcx_encrypion import *

reload(sys)
sys.setdefaultencoding('utf-8')

g_validFields = ["id","id2","m_name","m_aliasname","m_album_id","m_subalbum","m_track","m_audio_id","m_copyright","basic_version","basic_releasedate","basic_company","basic_intro","tags_category","tags_genre","tags_region","tags_lang","c_show_type","m_artists"]

class Task(object):
	#def __init__(self,config,connRun,curRun,connRes,curRes,taskurl="http://10.0.23.6:33327/add_task"):
	def __init__(self,config,connRun,curRun,connRes,curRes,taskurl="http://centerproxy.kuwo.cn/centerserver/add_task"):
		self.taskurl = taskurl
		self.connRun = connRun
		self.curRun = curRun
		self.connRes = connRes
		self.curRes = curRes
		self.config = config

	def getPicSource(self,src_url,tmp_local_dir):
		try:
			ret = download_url(tmp_local_dir,src_url)
			if ret != 0:
				return 0
			logging.info("downloaded picture path:" + tmp_local_dir)
			file_size = os.path.getsize(tmp_local_dir)
			if file_size <= 0:
				return 0
			dms_sig1,dms_sig2 = getSig(tmp_local_dir).strip().split(",")
			if not (dms_sig1 and dms_sig2):
				return 0
			sql = "select id from PicSource where file_sig1=%s and file_sig2=%s and version_editor>1 limit 1" % (dms_sig1,dms_sig2)
			cnt = self.curRun.execute(sql)
			if cnt > 0:
				ret = self.curRun.fetchone()
				self.connRun.commit()
				return ret[0]
			return 0
		except Exception,e:
			logging.info(str(e))
			traceback.print_exc()
			#pass

	def getAudioSource(self,src_url,tmp_local_dir):
		try:
			ret = download_url(tmp_local_dir,src_url)
			if ret != 0:
				return 0
			dms_sig1,dms_sig2 = getSig(tmp_local_dir).strip().split(",")
			if not (dms_sig1 and dms_sig2):
				return 0
			sql = "select id from AudioSource where file_sig1=%s and file_sig2=%s and version_editor>1 limit 1" % (dms_sig1,dms_sig2)
			cnt = self.curRun.execute(sql)
			if cnt > 0:
				ret = self.curRun.fethone()
				return ret[0]
			return 0
		except Exception,e:
			logging.info(str(e))
			#pass

	def getAudioSource2(self,file_sig1,file_sig2):
		try:
			sql = "select id from AudioSource where file_sig1=%s and file_sig2=%s and version_editor>1 limit 1" % (file_sig1,file_sig2)
			cnt = self.curRun.execute(sql)
			if cnt > 0:
				ret = self.curRun.fetchone()
				return ret[0]
			return 0
		except Exception,e:
			logging.info(str(e))
			#pass

	def getVideoSource2(self,file_sig1,file_sig2):
		try:
			sql = "select id from VideoSource where file_sig1=%s and file_sig2=%s and version_editor>1 limit 1" % (file_sig1,file_sig2)
			cnt = self.curRun.execute(sql)
			if cnt > 0:
				ret = self.curRun.fetchone()
				return ret[0]
			return 0
		except Exception,e:
			logging.info(str(e))
			#pass

	def genAudioSource(self,src_url,tmp_local_dir,tsk_count):
		try:
			AudioSource = {}
			###download
			ret = download_url(tmp_local_dir,src_url)
			if ret != 0:
				return AudioSource
			file_size = os.path.getsize(tmp_local_dir)
			dms_sig1,dms_sig2 = getSig(tmp_local_dir).strip().split(",")
			if not (dms_sig1 and dms_sig2):
				return AudioSource
			#dms_path = songFileProcess(dms_sig1,dms_sig2,self.config.configinfo["common"]["path_pre"],tmp_local_dir)
			AudioSource["file_size"] = ("%s") % file_size
			AudioSource["file_path"] = src_url.encode('utf-8')
			AudioSource["file_sig1"] = ("%s") % dms_sig1
			AudioSource["file_sig2"] = ("%s") % dms_sig2
			AudioSource["file_type"] = "audio_mass"
			if self.config.configinfo["MusicConst"].has_key("orig_ip") and self.config.configinfo["MusicConst"].has_key("targ_ip"):
				AudioSource["c_extparams"] = "orig_ip=%s&targ_ip=%s" % (self.config.configinfo["MusicConst"]["orig_ip"],self.config.configinfo["MusicConst"]["targ_ip"])
			AudioSource["file_format"] = src_url.split('.')[-1]
			audio_id = ("NEW_AudioSource_%s") % (tsk_count)
			AudioSource["id"] = audio_id
			return AudioSource
		except Exception,e:
			logging.info(str(e))
			#pass

	def check_lrc_exists(self,sig1,sig2,conn,cur):
		lrc_id = 0
		sql = '''select id from DMSRuntime.LyricsSource where file_sig1=%s and file_sig2=%s and version_editor>1''' % (sig1,sig2)
		cnt = cur.execute(sql)
		if cnt > 0:
			ret = cur.fetchone()
			lrc_id = ret[0]
		return lrc_id

	def genLyricsSource(self,info,lyric_type,index):
		try:
			#tmp_local_dir = self.config.configinfo["common"]["pre_path"] + "lyric/"
			tmp_local_dir = self.config.configinfo["common"]["pre_path"] + "pic/"
			if not os.path.exists(tmp_local_dir):
				os.makedirs(tmp_local_dir)
			http_pre_url = self.config.configinfo["common"]["http_local_pre_lyric"]
			LyricSource = {}
			resource_key = "lyric"
			if lyric_type == "lrcs":
				resource_key = "lyric"
			elif lyric_type == "lrc":
				resource_key = "lrc_line"
			elif lyric_type == "lrcx":
				resource_key = "lrc_word"
			else:
				return LyricSource,0
			if not info.has_key(resource_key):
				return LyricSource,0
			if resource_key == "lyric":
				if info[resource_key].strip(" ") == "":
					return LyricSource,0
				if not info["lyric"].strip(" ").isdigit():
					return LyricSource,0
			else:
				if info[resource_key] == 0:
					return LyricSource,0
			logging.info("%s process lyric %s" % (info["mid"],lyric_type))
			file_format = lyric_type
			#lyric_content = info["lyric"]
			###create file
			temp_file = "lyric_" + ''.join(random.sample(string.ascii_letters + string.digits, 8)) + "." + file_format
			strLrcUrl = http_pre_url + temp_file
			##request lyric
			#if lyric_type in ["lrcs","lrc"]:
			#	ret_url = resource_request(info["mid"],1)
			#elif lyric_type in ["lrcx"]:
			#	ret_url = resource_request_lrcx(info["mid"])
			ret_url = resource_request_lrcx(info["mid"])
			if len(ret_url) == 0:
				return LyricSource,0
			http_lyric_url = ""
			if lyric_type == "lrcs":
				if ret_url["code"] == 0 and ret_url["request"]["code"] == 0:
					if len(ret_url["request"]["data"]) > 0 and ret_url["request"]["data"].has_key("lyric_txt"):
						http_lyric_url = ret_url["request"]["data"]["lyric_txt"]["url"]
			elif lyric_type == "lrc":
				if ret_url["code"] == 0 and ret_url["request"]["code"] == 0:
					if len(ret_url["request"]["data"]) > 0 and ret_url["request"]["data"].has_key("lyric_line"):
						http_lyric_url = ret_url["request"]["data"]["lyric_line"]["url"]
			elif lyric_type == "lrcx":
				if ret_url["code"] == 0 and ret_url["request"]["code"] == 0:
					if len(ret_url["request"]["data"]) > 0 and ret_url["request"]["data"].has_key("lrcx"):
						http_lyric_url = ret_url["request"]["data"]["lrcx"]["url"]
			if http_lyric_url == "":
				return LyricSource,0
			ret = download_url(tmp_local_dir + temp_file, http_lyric_url)
			if ret != 0:
				return LyricSource,0
			f = open(tmp_local_dir + temp_file,"r")
			lyric_content = unicode(f.read(),errors="ignore")
			f.close()
			lyric_content = lyric_content.replace("\xef\xbb\xbf","",1)
			if lyric_type in ["lrc"]:
				f = open(tmp_local_dir + temp_file,"w+")
				f.write(lyric_content.encode("GB18030"))
				f.close()
			if lyric_type in ["lrcx"]:
				if lrcx_encryption(tmp_local_dir + temp_file, tmp_local_dir + temp_file) == False:
					logging.error("%s lrcx error" % (info["mid"]))
					return {},0

			str_content = lyric_content.replace('"','\\"')
			str_content_out = re.sub('\[.*?\]|<.*?>',"",str_content)
			str_content_out = str_content_out.replace("\r\n"," ").replace("\n"," ")
			if str_content_out == "":
				return LyricSource,0
				
			file_size = os.path.getsize(tmp_local_dir + temp_file)
			dms_sig1,dms_sig2 = getSig(tmp_local_dir + temp_file).strip().split(",")
			if not (dms_sig1 and dms_sig2):
				return LyricSource,0
			m_lrc_id = 0
			m_lrc_id = self.check_lrc_exists(dms_sig1,dms_sig2,self.connRun,self.curRun)
			if m_lrc_id > 0:
				return {},m_lrc_id
        		file_type = "lyric_%s" % (file_format)
			LyricSource["id"] = "NEW_LyricsSource_%s" % (index)
			LyricSource["m_name"] = info["m_name"]
			LyricSource["m_artist"] = info["m_artists"]
			LyricSource["file_size"] = ("%s") % file_size
			LyricSource["file_path"] = strLrcUrl
			LyricSource["file_sig1"] = dms_sig1
			LyricSource["file_sig2"] = dms_sig2
			LyricSource["file_type"] = file_type
			LyricSource["file_format"] = "%s" % (file_format)
			#LyricSource["m_exa_musicid"] = mid
			#LyricSource["m_is_exa"] = "1"
			LyricSource["m_content"] = str_content_out
			return LyricSource,0
		except Exception,e:
			logging.info(str(e))
			traceback.print_exc()
			pass
			return {},0

	def update_album_txid_task(self,id,tx_id):
	    try:
		logging.info(("update tencent relation id = %s.") % id)
		task_info = []
		# 生成Music描述
		Album = {}
		Album["id"] = "%s" % id
		Album["tx_albumid"] = "tm_%s" % tx_id
		task_info.append({"Album":Album})

		post_dict = {}
		post_dict["count"] = str(len(task_info))
		post_dict["priority"] = "8"
		post_dict["editor_id"] = "254"
		post_dict["timely"] = "0"
		post_dict["info"] = task_info

		post_json = json.dumps(post_dict)
		#print post_json
		logging.debug(post_json)
		#return False
		#'''
		f = urllib2.urlopen(
			url     = self.taskurl,
			data    = post_json
			)

		result = f.read()
		logging.info(str(result))
		js_ret = json.loads(result)
		logging.debug(result)
		task_id = 0
		if result.find("OK") >= 0:
		   task_id = js_ret["taskid"]

		if task_id == 0:
		    logging.error( ("fail when we modify tx_albumid, album id:%s, tx_albumid:%s.") % (id,tx_id) )
		    return task_id
		#print task_id
		#'''
		return task_id
	    except:
		traceback.print_exc()
		#pass

	def update_music_from_id_task(self,id,from_id):
	    try:
		logging.info(("update music relation id = %s.") % id)
		task_info = []
		# 生成Music描述
		Music = {}
		Music["id"] = "%s" % id
		Music["from_id"] = "tm_%s" % from_id
		task_info.append({"Music":Music})

		post_dict = {}
		post_dict["count"] = str(len(task_info))
		post_dict["priority"] = "8"
		post_dict["editor_id"] = "254"
		post_dict["timely"] = "0"
		post_dict["info"] = task_info

		post_json = json.dumps(post_dict)
		logging.debug(post_json)
		#return False
		#'''
		f = urllib2.urlopen(
			url     = self.taskurl,
			data    = post_json
			)

		result = f.read()
		logging.info(str(result))
		js_ret = json.loads(result)
		logging.debug(result)
		task_id = 0
		if result.find("OK") >= 0:
		   task_id = js_ret["taskid"]

		if task_id == 0:
		    logging.error( ("fail when we modify batch,music id:%s,music fromid:%s.") % (id,from_id) )
		    return task_id
		#print task_id
		#'''
		return task_id
	    except:
		traceback.print_exc()
		#pass

	def check_888Album(self,albumid,connSrc,curSrc):
		album_id = 0
		sql = '''select rid from Album888 where rid=%s''' % (albumid)
		cnt = curSrc.execute(sql)
		if cnt > 0:
			ret = curSrc.fetchone()
			album_id = ret["rid"]
		return album_id

	def checkDMSExists(self,connSrc,curSrc,result,type,tableSrc,relaTable):
		try:
			matched = False
			src_sql_class = sqlClass(connSrc,curSrc)
			###TODO:check if exists in database,return dict
			if type == "Album":
				if not result.has_key("m_name") or result["m_name"].strip() == "":
					where = "id=%s" % (result["id"])
					param = {}
					param["m_status"] = "%s" % g_status["retry_fail"]
					src_sql_class.mysqlUpdate(tableSrc,where,param)

					where = "from_aid=%s" % (result["id"])
					param = {}
					param["m_status"] = "%s" % g_status["retry_fail"]
					src_sql_class.mysqlUpdate(relaTable,where,param)
					matched = True
					#continue
				if not result.has_key("m_artists") or result["m_artists"].strip() == "":
					where = "id=%s" % (result["id"])
					param = {}
					param["m_status"] = "%s" % g_status["retry_fail"]
					src_sql_class.mysqlUpdate(tableSrc,where,param)

					where = "from_aid=%s" % (result["id"])
					param = {}
					param["m_status"] = "%s" % g_status["retry_fail"]
					src_sql_class.mysqlUpdate(relaTable,where,param)
					matched = True
					#continue

				is_manual = 100
				tx_albumid = ""
				tm_aids = []
				if result.has_key("tx_albumid") and int(result["tx_albumid"]) > 0:
					#tm_aid,is_manual = checkAlbumtm(result["tx_albumid"], self.connRun, self.curRun)
					g_connRelation = g_pool_Relation.connection()
					g_curRelation = g_connRelation.cursor()
					tm_aids = checkAlbumRelation(result["tx_albumid"], g_connRelation, g_curRelation)
					g_curRelation.close()
					g_connRelation.close()
				if len(tm_aids) == 0:
					kw_matched_albumid = 0
					kw_albumids = checkAlbumExists(result["m_name"],result["m_artists"],self.connRun,self.curRun,self.connRes,self.curRes)
					logging.info("match from name: albumid %s, tx_albumid %s " % (str(kw_albumids), tx_albumid))
					if len(kw_albumids) > 0:
						logging.info("album exists skip: %s" % (str(kw_albumids)))
						for kw_albumid in kw_albumids:
							###check whether has relation
							g_connRelation = g_pool_Relation.connection()
							g_curRelation = g_connRelation.cursor()
							qq_aids = checkAlbumRelationKW(kw_albumid, g_connRelation, g_curRelation)
							if len(qq_aids) > 0:
								logging.info("kw album-%s has relation %s" % (kw_albumid,str(qq_aids)))
							else:
								kw_matched_albumid = kw_albumid
							g_curRelation.close()
							g_connRelation.close()
						if kw_matched_albumid > 0:
							###update albumid and skip: MusicSrc,AlbumSrc
							where = "id=%s" % (result["id"])
							param = {}
							param["m_status"] = "%s" % g_status["has_matched"]
							param["m_album_id"] = "%s" % kw_matched_albumid
							src_sql_class.mysqlUpdate(tableSrc,where,param)
							'''
							where = "from_aid=%s" % (result["id"])
							param = {}
							param["m_album_id"] = "%s" % tm_aid
							src_sql_class.mysqlUpdate(relaTable,where,param)
							'''
							logging.info("match album: m_album_id-%s m_name-%s m_artists-%s   matched:%s" % (kw_matched_albumid,result["m_name"],result["m_artists"],tx_albumid))
							matched = True
					if kw_matched_albumid > 0:
						g_connRelation = g_pool_Relation.connection()
						g_curRelation = g_connRelation.cursor()
						insert_KWAlbumRelation(kw_matched_albumid,result["tx_albumid"],402,g_connRelation, g_curRelation)
						g_curRelation.close()
						g_connRelation.close()
						#continue
				elif len(tm_aids) == 1:
					tm_aid = tm_aids[0]
					logging.info("album exists skip: %s" % (tm_aid))
					###TODO:update albumid and skip: MusicSrc,AlbumSrc
					for id in result["sameids"]:
						where = "id=%s" % (id)
						param = {}
						param["m_status"] = "%s" % g_status["has_matched"]
						param["m_album_id"] = "%s" % tm_aid
						src_sql_class.mysqlUpdate(tableSrc,where,param)
					'''
					where = "from_aid=%s" % (result["id"])
					param = {}
					param["m_album_id"] = "%s" % tm_aid
					src_sql_class.mysqlUpdate(relaTable,where,param)
					if is_manual == 0:
						where = "from_aid=%s" % (result["id"])
						param = {}
						param["m_status"] = "%s" % g_status["editor_album"]
						src_sql_class.mysqlUpdate(relaTable,where,param)
					'''
					matched = True
					#continue
				elif len(tm_aids) > 1:
					logging.info("album exists multi skip: %s" % (str(tm_aids)))
					###TODO:update albumid and skip: MusicSrc,AlbumSrc
					for id in result["sameids"]:
						where = "id=%s" % (id)
						param = {}
						param["m_status"] = "%s" % g_status["relation_more"]
						#param["m_album_id"] = "%s" % tm_aid
						src_sql_class.mysqlUpdate(tableSrc,where,param)
						#'''
						where = "from_aid=\"%s\"" % (id)
						param = {}
						#param["m_album_id"] = "%s" % tm_aid
						param["m_status"] = "%s" % g_status["relation_more"]
						src_sql_class.mysqlUpdate(relaTable,where,param)
						#'''
					matched = True
			elif type == "Music":
				###:check name,m_artist
				if not result.has_key("m_name") or result["m_name"].strip() == "":
					where = "id=%s" % (result["id"])
					param = {}
					param["m_status"] = "%s" % g_status["retry_fail"]
					src_sql_class.mysqlUpdate(tableSrc,where,param)
					matched = True
					return matched
					#continue
				if not result.has_key("m_artists") or result["m_artists"].strip() == "":
					where = "id=%s" % (result["id"])
					param = {}
					param["m_status"] = "%s" % g_status["retry_fail"]
					src_sql_class.mysqlUpdate(tableSrc,where,param)
					matched = True
					return matched
					#continue
				if result.has_key("m_album_id") and self.check_888Album(result["m_album_id"],connSrc,curSrc) > 0:
					where = "id=%s" % (result["id"])
					param = {}
					param["m_status"] = "%s" % g_status["888album"]
					src_sql_class.mysqlUpdate(tableSrc,where,param)
					matched = True
					return matched
				###:check retry count
				retry_cnt = check_MusicSrc_try_count(result["id"],connSrc,curSrc)
				logging.info("retry cnt : %s - %s" % (result["id"], retry_cnt))
				if retry_cnt > 5:
					matched = True
					#continue
					return matched
				kw_id = 0
				from_id = ""
				album_name = ""
				if result.has_key("mid") and result["mid"] > 0:
					g_connRelation = g_pool_Relation.connection()
					g_curRelation = g_connRelation.cursor()
					kw_id = checkMusicRelation(result["mid"], g_connRelation, g_curRelation)
					g_curRelation.close()
					g_connRelation.close()
				#if result.has_key("kw_id") and result["kw_id"] > 0 and kw_id == 0:
				if result.has_key("kw_id") and result["kw_id"] > 0:
					logging.info("need update kw_id:%s" % (result["kw_id"]))
				else:
					if kw_id > 0:
						logging.info("Music exists skip: %s" % (kw_id))
						###TODO:update albumid and skip: MusicSrc,AlbumSrc
						where = "id=%s" % (result["id"])
						param = {}
						param["m_status"] = "%s" % g_status["has_matched"]
						param["kw_id"] = "%s" % kw_id
						src_sql_class.mysqlUpdate(tableSrc,where,param)
						matched = True
						#continue
					else:
						if result.has_key("m_album_id"):
							is_editor = 0
							tx_aid = 0
							album_name = ""
							if int(result["m_album_id"]) > 0:
								tx_aid,is_editor,album_name = checkEditorAlbum(result["m_album_id"],self.connRun,self.curRun)
							kw_ids = checkMusicMatch(album_name,result["m_name"],result["m_artists"],result["basic_version"],result["version2"],True) 
							if kw_ids is not None and len(kw_ids) > 0:
								g_connRelation = g_pool_Relation.connection()
								g_curRelation = g_connRelation.cursor()
								for tid in kw_ids:
									tx_id = checkMusicRelationKW(tid,g_connRelation, g_curRelation)
									if tx_id > 0:
										continue
									if checkOnline("Music",tid,self.connRun,self.curRun) > 0:
										kw_id = tid
										break
									else:
										kw_id = tid
								g_curRelation.close()
								g_connRelation.close()
								#if kw_id == 0:
								#	kw_id = kw_ids[0]
							if kw_id == 0:
								kw_id,from_id = checkMusicExists(result["m_album_id"],result["m_name"],result["m_artists"],result["basic_version"],result["version2"],self.connRun,self.curRun,self.connRes,self.curRes)
								tx_id = 0
								if kw_id > 0:
									g_connRelation = g_pool_Relation.connection()
									g_curRelation = g_connRelation.cursor()
									tx_id = checkMusicRelationKW(kw_id,g_connRelation, g_curRelation)
									g_curRelation.close()
									g_connRelation.close()
									if tx_id > 0:
										kw_id = 0
							else:
								from_id = getMusicFromid(kw_id,self.connRun,self.curRun)
							logging.info("match from name: mid %s, from_id %s, is_editor %s " % (kw_id, from_id, is_editor))
							if is_editor == 1:
								if kw_id > 0:
									where = "id=%s" % (result["id"])
									param = {}
									param["m_status"] = g_status["has_matched"]
									param["editor_album"] = g_status["editor_album"]
									param["kw_id"] = kw_id
									src_sql_class.mysqlUpdate(tableSrc,where,param)
									matched = True
								else:
									where = "id=%s" % (result["id"])
									param = {}
									#param["m_status"] = g_status["match_first"]
									param["editor_album"] = g_status["editor_album"]
									param["kw_id"] = kw_id
									src_sql_class.mysqlUpdate(tableSrc,where,param)
									matched = False
								#if kw_id > 0:
								#	self.update_music_from_id_task(kw_id,result["mid"])
								#matched = True
								#continue
							if kw_id > 0:
								g_connRelation = g_pool_Relation.connection()
								g_curRelation = g_connRelation.cursor()
								tm_other_id = insert_KWRelation(kw_id,result["mid"],402,g_connRelation, g_curRelation)
								#if tm_other_id > 0:
								#	insert_TencentRepeat(result["mid"],tm_other_id,g_connRelation, g_curRelation)
								g_curRelation.close()
								g_connRelation.close()
								#if from_id.find("tx_") != -1 or from_id.find("tm_") != -1:
								#	where = "id=%s" % (result["id"])
								#	param = {}
								#	param["m_status"] = "%s" % g_status["has_matched"]
								#	param["kw_id"] = "%s" % kw_id
								#	src_sql_class.mysqlUpdate(tableSrc,where,param)
								#else:
								where = "id=%s" % (result["id"])
								param = {}
								param["m_status"] = "%s" % g_status["has_matched"]
								param["kw_id"] = "%s" % kw_id
								param["matched_other_mid"] = "%s" % tm_other_id
								src_sql_class.mysqlUpdate(tableSrc,where,param)
								#self.update_music_from_id_task(kw_id,result["mid"])
								g_connTMApi = g_pool_TMApi.connection()
								g_curTMApi = g_connTMApi.cursor()
								if checkTMApiStatus2(kw_id,g_connTMApi,g_curTMApi) == 2:
									sendTaskOnline(kw_id,"Music")
								g_curTMApi.close()
								g_connTMApi.close()
								logging.info("matched music: m_album_id-%s m_name-%s m_artists-%s kw_id-%s" % (result["m_album_id"],result["m_name"],result["m_artists"],kw_id))
								matched = True
								#continue
			elif type == "Artist":
				###:check name,m_artist
				if not result.has_key("m_name") or result["m_name"].strip() == "":
					where = "id=%s" % (result["id"])
					param = {}
					param["m_status"] = "%s" % g_status["wrong_info"]
					src_sql_class.mysqlUpdate(tableSrc,where,param)
					matched = True

				###:check exists
				g_connRelation = g_pool_Relation.connection()
				g_curRelation = g_connRelation.cursor()
				kw_artistids = checkArtistRelation(result["tmeid"],g_connRelation,g_curRelation)
				kw_artistid = 0
				if len(kw_artistids) > 0:
					kw_artistid = kw_artistids[0]
				else:
					kw_artistid = getArtistIdFromName(result["m_name"],self.connRun,self.curRun,connSrc,curSrc)
					insert_KWArtistRelation(kw_artistid,result["tmeid"],402,g_connRelation,g_curRelation)
				g_curRelation.close()
				g_connRelation.close()
				if kw_artistid > 0:
					where = "id=%s" % (result["id"])
					param = {}
					param["m_status"] = "%s" % g_status["has_matched"]
					param["m_artist_id"] = "%s" % kw_artistid
					src_sql_class.mysqlUpdate(tableSrc,where,param)
					logging.info("Artist matched tmeid-%s kwid-%s" % (result["tmeid"],kw_artistid))
					matched = True
			return matched
		except Exception,e:
			traceback.print_exc()


	def genPicSource(self,t_task,src_url,tmp_local_dir,path_pre,http_pre):
		try:
			PicSource = {}
			ret = download_url(tmp_local_dir,src_url)
			if ret != 0:
				return PicSource
			logging.info("downloaded picture path:" + tmp_local_dir)
			if os.path.getsize(tmp_local_dir) == 0:
				return PicSource
			Im = Image.open(tmp_local_dir)
			Im_format = Im.format
			if Im_format == "JPEG":
				Im_format = "jpg"
			elif Im_format == "PNG":
				Im_format = "png"
			elif Im_format == "BMP":
				Im_format = "bmp"
			else:
				Im_format = "jpg"

			dms_sig1,dms_sig2 = getSig(tmp_local_dir).strip().split(",")
			if not (dms_sig1 and dms_sig2):
				return PicSource
			sigpath = ImageFileProcess(dms_sig1,dms_sig2,path_pre,tmp_local_dir).lstrip("/")
			PicSource = {}
			PicSource["file_size"] = str(os.path.getsize(path_pre + sigpath))
			PicSource["file_path"] = http_pre + sigpath
			PicSource["file_sig1"] = ("%s") % dms_sig1
			PicSource["file_sig2"] = ("%s") % dms_sig2
			PicSource["c_extparams"] = ("nozcopy=1&scale=1")
			
			if t_task == "Album":
				PicSource["file_type"] = "pic_bat_album"
			elif t_task == "Artist":
				PicSource["file_type"] = "pic_bat_artist"
			elif t_task == "Music":
				PicSource["file_type"] = "pic_nor_music"
			elif t_task == "Video":
				PicSource["file_type"] = "pic_nor_mv"
			PicSource["file_format"] = Im_format
			PicSource["id"] = "NEW_PicSource"
			return PicSource
		except Exception,e:
			traceback.print_exc()
			logging.info(str(e))
			return {}
			pass

	def genTaskInfo(self,connSrc,curSrc,task_type,tableSrc,relaTable,checkInDMS,config,info_arr):
		try:
			task_info = []
			tsk_count = 0
			ids_task = set()
			priority = 8
			for info in info_arr:
				print info
				taskSingle = {}
				c_show_type = 10
				old_lrc = 0
				old_lrcx = 0
				old_lrcs = 0
				#print info
				if not (info.has_key("new_request") and info["new_request"] == 1):
					if (checkInDMS and self.checkDMSExists(connSrc,curSrc,info,task_type,tableSrc,relaTable)):
						continue
				elif config.has_key("id2") and info.has_key(config["id2"]) and info[config["id2"]] != "" and info[config["id2"]] != "0" and info[config["id2"]] != 0 and info[config["id2"]] is not None and info[config["id2"]] != "None":
					logging.info("update kw id %s" % (info[config["id2"]]))
					###check audio and check lyrics
					old_lrc,old_lrcx,old_lrcs = checkMusicLyrics(info[config["id2"]],self.connRun,self.curRun)
					logging.info("update kw id %s - lrc:%s-lrcx:%s,lrcs:%s" % (info[config["id2"]],old_lrc,old_lrcx,old_lrcs))
				if task_type == "Music" and info.has_key("m_album_id") and info["m_album_id"] > 0:
					if not check_DMS_Album_status(info["m_album_id"],self.connRun,self.curRun):
						continue
				for k,v in config.items():
					if k in ["file_size","file_sig1","file_sig2","file_path","file_type","c_extparams","file_format","sameids","new_request","tme_artist_ids","duration","lyric","mv_id","m_v_pic_id","lrc_line","lrc_word","audioproduct_path","lrc"]:
						continue
					if k == "m_artists":###: split ';','###',',','&'
						if config.has_key("m_artist_ids") and info[config["m_artist_ids"]].strip() != "":
							continue
						taskSingle[k] = list(getArtistIdsFromName(info[v].strip(),"/",self.connRun,self.curRun,connSrc,curSrc))
						if len(taskSingle[k]) < 1:
							taskSingle[k] = list(getArtistIdsFromName(info[v].strip(),";",self.connRun,self.curRun,connSrc,curSrc))
						if len(taskSingle[k]) < 1:
							taskSingle[k] = list(getArtistIdsFromName(info[v].strip(),"###",self.connRun,self.curRun,connSrc,curSrc))
						if len(taskSingle[k]) < 1:
							taskSingle[k] = list(getArtistIdsFromName(info[v].strip(),",",self.connRun,self.curRun,connSrc,curSrc))
						if len(taskSingle[k]) < 1:
							taskSingle[k] = list(getArtistIdsFromName(info[v].strip(),"、",self.connRun,self.curRun,connSrc,curSrc))
						if len(taskSingle[k]) < 1:
							taskSingle[k] = list(getArtistIdsFromName(info[v].strip(),"|",self.connRun,self.curRun,connSrc,curSrc))
						if len(taskSingle[k]) < 1:
							taskSingle[k] = list(getArtistIdsFromName(info[v].strip(),"&",self.connRun,self.curRun,connSrc,curSrc))
						if len(taskSingle[k]) == 0:
							return {},ids_task
						continue
					if k == "m_artist_ids": ###default sepa=;
						#taskSingle["m_artists"] = info[v].strip().split(";")
						if info[v].strip() != "" and info[v].strip() != "0":
							taskSingle["m_artists"] = info[v].strip().split("&")
						continue
					if k == "m_record_pic_id" or k == "m_pic_id":
						###tmp dir,genarate the file name by rand or use source file name
						tmp_local_dir = self.config.configinfo["common"]["pre_path"] + "pic/"
						if not os.path.exists(tmp_local_dir):
							os.makedirs(tmp_local_dir)
						source_url = ""
						source_fmt = ""
						if info.has_key("tx_albumid"):
							if int(info["tx_albumid"]) > 0:
								ret_url = resource_request(int(info["tx_albumid"]),2)
								if ret_url is not None and ret_url["code"] == 0 and ret_url["download_url"]["code"] == 0:
									source_url = ret_url["download_url"]["data"]["url"]
									source_fmt = ret_url["download_url"]["data"]["file_type"]
						if source_url == "":
							if info[v] == "":
								taskSingle[k] = "%s" % (1594440)
								continue
							if self.config.configinfo["common"].has_key("http_peer_pre") and info[v].find("http") == -1:
								source_url = self.config.configinfo["common"]["http_peer_pre"] + info[v]
						
						tmp_local_dir += "%s_%s" % (task_type,''.join(random.sample(string.ascii_letters + string.digits, 8)))
						pic_id = 0
						pic_id = self.getPicSource(source_url,tmp_local_dir)
						if pic_id == 0:
							taskSingle[k] = "NEW_PicSource"
							PicSource = self.genPicSource(task_type,source_url,tmp_local_dir,self.config.configinfo["common"]["pre_path"] + "pic/",self.config.configinfo["common"]["http_local_pre"])
							if len(PicSource) < 7:
								logging.info("pic source error.continue")
								taskSingle[k] = "1594440"
								#return []
							else:
								taskSingle[k] = "NEW_PicSource"
								task_info.append({"PicSource":PicSource})
						else:
							taskSingle[k] = "%s" % (pic_id)
					elif k == "m_audio_id":
						if v == "m_audio_url" and info.has_key("file_sig1") and info.has_key("file_sig2"):
							audio_id = self.getAudioSource2(info["file_sig1"],info["file_sig2"])
							#audio_id =0
							if audio_id > 0:
								taskSingle[k] = "%s" % (audio_id)
							else:
								AudioSource = {}
								AudioSource["file_size"] = ("%s") % info["file_size"]
								AudioSource["file_path"] = info["file_path"].strip("/")
								AudioSource["file_sig1"] = ("%s") % info["file_sig1"]
								AudioSource["file_sig2"] = ("%s") % info["file_sig2"]
								#AudioSource["file_type"] = "audio_mass2"
								AudioSource["file_type"] = "%s" % (info["file_type"])
								AudioSource["c_extparams"] = ("%s") % info["c_extparams"]
								AudioSource["file_format"] = ("%s") % info["file_format"]
								audio_id = ("NEW_AudioSource_%s") % (tsk_count)
								AudioSource["id"] = "%s" % (audio_id)
								if info.has_key("audioproduct_path"):
									AudioSource["audioproduct_path"] = ("%s") % info["audioproduct_path"]
								#task_info.append({"AudioSource":AudioSource})
								taskSingle[k] = "%s" % (audio_id)
								if old_lrcs > 0:
									AudioSource["m_lrcs"] = "%s" % (old_lrcs)
								if old_lrc > 0:
									AudioSource["m_lrc"] = "%s" % (old_lrc)
								if old_lrcx > 0:
									AudioSource["m_lrcx"] = "%s" % (old_lrcx)

								if info.has_key("lrc") and str(info["lrc"]).isdigit():
									LyricSource,m_lrc_id = self.genLyricsSource(info,"lrcs",tsk_count)
									if old_lrcs > 0 and m_lrc_id == 0:
										m_lrc_id = old_lrcs
									if m_lrc_id > 0:
										AudioSource["m_lrcs"] = "%s" % (m_lrc_id)
									elif len(LyricSource) > 0:
										logging.info("%s has lyric lrcs" % (info["id"]))
										task_info.append({"LyricsSource":LyricSource})
										AudioSource["m_lrcs"] = "NEW_LyricsSource_%s" % (tsk_count)
										tsk_count += 1
									else:
										logging.info("%s no lyric lrcs" % (info["id"]))
								if info.has_key("lrc_line") and str(info["lrc_line"]).isdigit():
									LyricSource,m_lrc_id = self.genLyricsSource(info,"lrc",tsk_count)
									if old_lrc > 0 and m_lrc_id == 0:
										m_lrc_id = old_lrc
									if m_lrc_id > 0:
										AudioSource["m_lrc"] = "%s" % (m_lrc_id)
									elif len(LyricSource) > 0:
										logging.info("%s has lyric lrc" % (info["id"]))
										task_info.append({"LyricsSource":LyricSource})
										AudioSource["m_lrc"] = "NEW_LyricsSource_%s" % (tsk_count)
										tsk_count += 1
									else:
										logging.info("%s no lyric lrc" % (info["id"]))
								if info.has_key("lrc_word") and str(info["lrc_word"]).isdigit():
									LyricSource,m_lrc_id = self.genLyricsSource(info,"lrcx",tsk_count)
									if old_lrcx > 0 and m_lrc_id == 0:
										m_lrc_id = old_lrcx
									if m_lrc_id > 0:
										AudioSource["m_lrcx"] = "%s" % (m_lrc_id)
									elif len(LyricSource) > 0:
										logging.info("%s has lyric lrcx" % (info["id"]))
										task_info.append({"LyricsSource":LyricSource})
										AudioSource["m_lrcx"] = "NEW_LyricsSource_%s" % (tsk_count)
										tsk_count += 1
									else:
										logging.info("%s no lyric lrcx" % (info["id"]))
								task_info.append({"AudioSource":AudioSource})
						elif v == "m_audio_url" and not info.has_key("file_sig1"):
							###tmp dir,genarate the file name by rand or use source file name
							tmp_local_dir = self.config.configinfo["common"]["pre_path"] + "audio/"
							if not os.path.exists(tmp_local_dir):
								os.makedirs(tmp_local_dir)
							tmp_local_dir += info[v].split("/")[-1]
							audio_id = self.getAudioSource(info[v],tmp_local_dir)
							if audio_id == 0:
								taskSingle[k] = "NEW_AudioSource_%s" % (tsk_count)
								AudioSource = {}
								AudioSource = self.genAudioSource(info[v],tmp_local_dir,tsk_count)
								if len(AudioSource) < 7:
									logging.info("audio source error")
									return False
								task_info.append({"AudioSource":AudioSource})
							else:
								taskSingle[k] = "%s" % (audio_id)
					elif k == "m_v_source_id":
						tmp_local_dir = self.config.configinfo["common"]["pre_path"] + "pic/"
						if not os.path.exists(tmp_local_dir):
							os.makedirs(tmp_local_dir)
						if v == "m_origurl" and info.has_key("file_sig1") and info.has_key("file_sig2"):
							video_id = self.getVideoSource2(info["file_sig1"],info["file_sig2"])
							if video_id > 0:
								taskSingle[k] = "%s" % (video_id)
							else:
								VideoSource = {}
								VideoSource["file_size"] = ("%s") % info["file_size"]
								VideoSource["file_path"] = info["file_path"].strip("/")
								VideoSource["file_sig1"] = ("%s") % info["file_sig1"]
								VideoSource["file_sig2"] = ("%s") % info["file_sig2"]
								VideoSource["file_type"] = "%s" % (info["file_type"])
								VideoSource["c_extparams"] = ("%s") % info["c_extparams"]
								VideoSource["file_format"] = ("%s") % info["file_format"]
								VideoSource["m_name"] = ("%s") % info["m_name"]
								VideoSource["m_artist"] = ("%s") % info["m_artist"]
								
								video_id = ("NEW_VideoSource_%s") % (tsk_count)
								VideoSource["id"] = "%s" % (video_id)
								taskSingle[k] = "%s" % (video_id)
								if info.has_key("m_pic_url") and info["m_pic_url"] != "":
									tmp_local_dir += "%s_%s" % (task_type,''.join(random.sample(string.ascii_letters + string.digits, 8)))
									pic_id = 0
									pic_id = self.getPicSource(info["m_pic_url"],tmp_local_dir)
									if pic_id == 0:
										VideoSource["m_pic_id"] = "NEW_PicSource_%s" % (tsk_count)
										PicSource = self.genPicSource(task_type,info["m_pic_url"],tmp_local_dir,self.config.configinfo["common"]["pre_path"] + "pic/",self.config.configinfo["common"]["http_local_pre"])
										PicSource["id"] = "NEW_PicSource_%s" % (tsk_count)
										if len(PicSource) < 7:
											logging.info("pic source error.continue")
											VideoSource["m_pic_id"] = "1594440"
											#return []
										else:
											VideoSource["m_pic_id"] = "NEW_PicSource_%s" % (tsk_count)
											task_info.append({"PicSource":PicSource})
									else:
										VideoSource["m_pic_id"] = "%s" % (pic_id)
								task_info.append({"VideoSource":VideoSource})
					elif k == "basic_releasedate":
						#print info[v]
						if info[v].isdigit():
							taskSingle[k] = time.strftime("%Y-%m-%d",time.localtime(int(info[v])))
						elif info[v] != "":
							taskSingle[k] = "%s" % info[v].strip()[0:10]
						continue
					elif k == "timing_online":
						#print k,v
						#print info[v]
						if info.has_key(v) and info[v] is not None:
							if info[v] != "0000-00-00 00:00:00":
								c_show_type = 0
								taskSingle[k] = "%s" % info[v]
							else:
								c_show_type = 10
						continue
					elif k == "id":
						if config.has_key("id2") and info.has_key(config["id2"]) and info[config["id2"]] != "" and info[config["id2"]] != "0" and info[config["id2"]] != 0 and info[config["id2"]] is not None and info[config["id2"]] != "None":
							taskSingle[k] = "%s" % info[config["id2"]]
							###check audio and check lyrics
							old_lrc,old_lrcx,old_lrcs = checkMusicLyrics(info[config["id2"]],self.connRun,self.curRun)
						else:
							taskSingle[k] = "NEW_%s_%s" % (task_type,tsk_count)
					elif k == "c_show_type":
						#if int(info[v]) == 1:
						#	taskSingle[k] = "%s" % (0)
						#else:
						#	taskSingle[k] = "%s" % (11)
						#taskSingle[k] = "%s" % info[v]
						#taskSingle[k] = "%s" % 10
						continue
					elif k in ["id2","c_create_editor","from_aid","m_status","from_mvid"]:
						continue
					elif k == "priority":
						if priority < int(info[v]):
							priority = info[v]
						continue
					elif k == "tx_albumid":
						if int(info[v]) > 0:
							taskSingle[k] = "tm_%s" % info[v]
						continue
					elif k == "tx_artistid":
						if int(info[v]) > 0:
							taskSingle[k] = "tm_%s" % info[v]
						continue
					elif k == "from_id":
						if int(info[v]) > 0:
							taskSingle[k] = "tm_%s" % info[v]
						continue
					elif k == "hf_type":
						if int(info[v]) > 0:
							c_show_type = 3
					elif info.has_key(v) and info[v] is not None:
						taskSingle[k] = "%s" % info[v]
				if task_type == "Music":
					c_batch = 0
					if info.has_key("m_album_id") and info["m_album_id"] > 0:
						c_batch,company,cp_id = check_DMS_Album_batch(info["m_album_id"],self.connRun,self.curRun)
					for k,v in self.config.configinfo["MusicConst"].items():
						if k in ["c_batch","c_show_type"]:
							if k == "c_show_type":
								taskSingle[k] = "%s" % c_show_type
							elif k == "c_batch":
								if c_batch == 888 or c_batch == 117:
									taskSingle[k] = "%s" % c_batch
								else:
									taskSingle[k] = "%s" % v
							else:
								taskSingle[k] = "%s" % v
				if task_type == "Artist":
					if c_show_type == 10:
						c_show_type = 0
					for k,v in self.config.configinfo["ArtistConst"].items():
						if k in ["c_batch","c_show_type"]:
							#taskSingle[k] = "%s" % v
							if k == "c_show_type":
								taskSingle[k] = "%s" % c_show_type
							else:
								taskSingle[k] = "%s" % v
				if task_type == "Album":
					if c_show_type == 10:
						if info.has_key("company_id") and info["company_id"] == 13493:
							c_show_type = 10
						elif info.has_key("basic_company") and info["basic_company"].find("阅文") != -1:
							c_show_type = 10
						else:
							c_show_type = 0
					for k,v in self.config.configinfo["AlbumConst"].items():
						if k in ["c_batch","c_show_type"]:
							#taskSingle[k] = "%s" % v
							if k == "c_show_type":
								taskSingle[k] = "%s" % c_show_type
							else:
								taskSingle[k] = "%s" % v
							#if k == "c_show_type":
							#	taskSingle[k] = "%s" % c_show_type
							#else:
							#	taskSingle[k] = "%s" % v
				if task_type == "Video":
					if c_show_type == 10:
						c_show_type = 0
					for k,v in self.config.configinfo["VideoConst"].items():
						if k in ["c_batch","c_show_type"]:
							#taskSingle[k] = "%s" % v
							if k == "c_show_type":
								taskSingle[k] = "%s" % c_show_type
							else:
								taskSingle[k] = "%s" % v
				if len(taskSingle) > 0:
					task_info.append({"%s" % task_type:taskSingle})
				for tid in info["sameids"]:
					ids_task.add(tid)
				tsk_count += 1

			post_dict = {}
			post_dict["count"] = "%s" % (len(task_info))
			post_dict["info"] = task_info
			#logging.info(str(post_dict))
			for k,v in self.config.configinfo["common"].items():
				if k in ["editor_id","priority"]:
					if k == "priority":
						if task_type in ["Artist","Album"]:
							post_dict[k] = "%s" % 9
						else:
							post_dict[k] = "%s" % priority
					else:
						post_dict[k] = "%s" % v
			return post_dict,ids_task
		except Exception,e:
			traceback.print_exc()
			logging.info(str(e))
			#pass

	def getRealIdfromTask(self,taskid,table_type):
		sql = "select table_id,status from DMSTask.Action where task_id=%s and `table`=\"%s\" limit 1" %(taskid,table_type)
		cnt = self.curRun.execute(sql)
		if cnt > 0:
			ret = self.curRun.fetchone()
			self.connRun.commit()
			table_id = ret["table_id"]
			status = ret["status"]
			logging.info("table id : %s, status : %s" % (table_id,status))
			return table_id,status
		else:
			return 0,False

	def disonlineTask(self, task_type, configDict, ids):
		###c_show_type 11
		task_info = []
		for id in ids:
			info = '{"%s":{"c_show_type":"11","id":"%s"}}' % (task_type,id)
			task_info.append(info)
		post_dict = {}
		post_dict["count"] = "%s" % (len(task_info))
		post_dict["info"] = task_info
		for k,v in configDict["common"].items():
			if k in ["editor_id","priority"]:
				post_dict[k] = "%s" % v
		return post_dict

	def sendTask(self, info):
		try:
			task_id = 0
			#task_id = 30884833 #Music Album
			#task_id = 30916210 #Artist
			if len(info) == 0:
				return task_id
			post_json = json.dumps(info)
			logging.info(str(post_json))
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
			logging.info(str(result))
			js_ret = json.loads(result)
			if result.find("OK") >= 0:
				task_id = int(js_ret["taskid"])
			#'''
			return task_id
		except Exception,e:
			logging.info(str(e))
			traceback.print_exc()
			return task_id

