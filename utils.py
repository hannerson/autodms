#!/bin/python
#coding=utf-8

import sys,os
import requests
import json
import shutil
import time,datetime
import MySQLdb
import traceback
import urllib2
import urllib
import re
import hashlib
import struct
from PIL import Image
from logger import *

g_status = {"default":0,"dispatch":1,"sig_ok":2,"artist_ok":3,"task_send":4,"artist_no":5,"task_fail":6,"task_suc":7,"sig_fail":8,"retry_fail":9,"has_matched":10,"editor_album":11,"match_first":12}

def artist_split(artists):
    artistset = set()
    if artists.find('/') != -1:
       	artistlist = artists.strip().split("/")
    elif artists.find('###') != -1:
       	artistlist = artists.strip().split("###")
    elif artists.find(';') != -1:
       	artistlist = artists.strip().split(";")
    elif artists.find('|') != -1:
       	artistlist = artists.strip().split("|")
    elif artists.find(',') != -1:
       	artistlist = artists.strip().split(",")
    elif artists.find('、') != -1:
       	artistlist = artists.strip().split("、")
    else:
       	artistlist = artists.strip().split("###")
    for a in artistlist:
	artistset.add(a.strip())
    return artistset

def artist_split_list(artistStr,sepa):
	artists = []
	arrs = artistStr.strip().split(sepa)
	for arr in arrs:
		artists.append(arr.strip())
		#print arr
	return artists

def sendTaskOnline(mid,type):
    try:
        task_id = 0
        post_dict = {}
        Music = {}
        Music["id"] = "%s" % (mid)
        Music["c_show_type"] = "%s" % (0)
        info = []
        info.append({type:Music})
        post_dict["count"] = "%s" % (len(info))
        post_dict["info"] = info
        post_dict["priority"] = "%s" % (7)
        post_dict["editor_id"] = "%s" % (254)
        post_json = json.dumps(post_dict)
        print post_json
        #'''
        f = urllib2.urlopen(
            #url     = "http://103.79.26.21:33327/add_task",
            url     = "http://centerserver.kuwo.cn/add_task",
            data    = post_json
            )

        result = f.read()
        print result
        js_ret = json.loads(result)
        if result.find("OK") >= 0:
            task_id = int(js_ret["taskid"])
        #'''
        return task_id
    except Exception,e:
        traceback.print_exc()
        #pass

def checkTMApiStatus(mid,conn,cur):
	show_status = 0
	sql = '''select show_status from TMApi where track_id="%s"''' % (mid)
	cnt = cur.execute(sql)
	if cnt > 0:
		ret = cur.fetchone()
		show_status = ret["show_status"]
	return show_status

def checkTMApiStatus2(mid,conn,cur):
	show_status = 0
	sql = '''select ctif_available_status from CenterApi where track_id="%s"''' % (mid)
	cnt = cur.execute(sql)
	if cnt > 0:
		ret = cur.fetchone()
		show_status = ret["ctif_available_status"]
	return show_status

def getTrimMsg(msg):
	# 删除特殊字符
	#p1 = re.compile("[\(\)\[\] ,\.\{\}\- ]|（|）|【|】|。|　| |，|✅|～")
	# 删除特殊字符和括号中的内容
	p2 = re.compile("\(.*?\)|\[.*?\]|（.*?）|【.*?】|\{.*?\}|《.*?》|『.*?』")
	if p2.sub("", msg) == "":
		#msg = p1.sub("", msg)
		return msg
	else:
		msg = p2.sub("", msg)
		#msg = p1.sub("", msg)	
	return msg

def calTime(date1,date2):
	date1=time.strptime(date1,"%Y-%m-%d")
    	date2=time.strptime(date2,"%Y-%m-%d")

	date1=datetime.datetime(date1[0],date1[1],date1[2])
    	date2=datetime.datetime(date2[0],date2[1],date2[2])

	return (date1-date2).days

def loadArtistTop(filename):
	artistSet = set()
	f = open(filename,"r")
	for line in f:
		arr = line.strip().split("\t")
		if len(arr) < 2:
			continue
		artistSet.add(arr[1].strip())
	f.close()
	return artistSet

def checkOnline(type,tid,connRun,curRun):
	c_show_type = -1
	sql = '''select c_show_type from DMSRuntime.%s where id=%s''' % (type,tid)
	cnt = curRun.execute(sql)
	if cnt > 0:
		ret = curRun.fetchone()
		if int(ret[0]) == 0 or int(ret[0]) == 14:
			c_show_type = 1
		else:
			c_show_type = 0
	return c_show_type

def update_MusicSrc_try_count(id,connSrc,curSrc):
	sql = '''update MusicSrc set retry_count=retry_count+1 where id=%s''' % (id)
	cnt = curSrc.execute(sql)
	connSrc.commit()
	logging.info(sql)
	if cnt > 0:
		connSrc.commit()

def check_DMS_Album_status(album_id,conn,cur):
	editor_status = 0
	sql = '''select count(*) from DMSRuntime.Album where id=%s and version_pub=version_editor''' % (album_id)
	cnt = cur.execute(sql)
	if cnt > 0:
		conn.commit()
		editor_status = 1
	return editor_status

def check_DMS_Album_batch(album_id,conn,cur):
	c_batch = 0
	company = ""
	cp_id = 0
	sql = '''select c_batch,basic_company,company_id from DMSRuntime.Album where id=%s''' % (album_id)
	cnt = cur.execute(sql)
	if cnt > 0:
		conn.commit()
		ret = cur.fetchone()
		c_batch = ret[0]
		company = ret[1]
		cp_id = ret[2]
	return c_batch,company,cp_id

def check_MusicSrc_try_count(id,connSrc,curSrc):
	retry_cnt = 0
	sql = '''select retry_count from MusicSrc where id=%s''' % (id)
	cnt = curSrc.execute(sql)
	connSrc.commit()
	logging.info(sql)
	if cnt > 0:
		ret = curSrc.fetchone()
		retry_cnt = ret["retry_count"]
		if retry_cnt > 5:
			sql = '''update MusicSrc set m_status=%s where id=%s''' % (g_status["retry_fail"],id)
			cnt = curSrc.execute(sql)
			logging.info(sql)
			if cnt > 0:
				connSrc.commit()
	return retry_cnt

def artist_split_set(artistStr,sepa):
	artists = set()
	arrs = artistStr.strip().split(sepa)
	for arr in arrs:
		artists.add(arr.strip())
	return artists

#从艺人列表拼接艺人字符串，以";"分隔
def artists_get_name(artists):
	artist_str = ''
	artist_list = []
	for artist in artists:
		artist_list.append(artist)

	artist_str = ';'.join(artist_list)
	return artist_str

def huangfanCheck(name, artist):
	action = 'http://huangfan.kuwo.cn:8180/api/highrisk'
	query = {
		'name' : name,
		'artist' : artist,
	}
	connection = requests.post(
		action,
		data=query,
		timeout=DEFAULT_TIMEOUT
	)
	connection.encoding = "UTF-8"
	data = json.loads(connection.text)
	return data

#取得文件的sig值
def getSig(filename):
	curdir = os.path.split(os.path.realpath(__file__))[0]
	output = os.popen(curdir + "/mkylnewsig \"%s\" " % (filename))
	sig = output.read()
	return sig

def checkMusicMatch(albumname,musicname,artists,version,version2,onlyMusic):
	try:
		music_id = []
		artists = artists.replace(" ","").lower()
		if artists.find('###') != -1:
			artistlist = artists.strip().split("###")
		elif artists.find('/') != -1:
			artistlist = artists.strip().split("/")
		elif artists.find(';') != -1:
			artistlist = artists.strip().split(";")
		elif artists.find('|') != -1:
			artistlist = artists.strip().split("|")
		elif artists.find(',') != -1:
			artistlist = artists.strip().split(",")
		elif artists.find('、') != -1:
			artistlist = artists.strip().split("、")
		else:
			artistlist = artists.strip().split("###")
		artistlist.sort()
		data = {}
		data["song"] = musicname
		data["id"] = ""
		data["artist"] = ",".join(artistlist)
		data["album"] = albumname
		if version2 != "":
			data["version"] = version2
		else:
			data["version"] = version
		#match_url = "http://101.36.137.21:8888/music_match?%s" % (urllib.urlencode(data))
		match_url = "http://10.1.4.9:8888/music_match?%s" % (urllib.urlencode(data))
		print match_url
		f = urllib2.urlopen(
			url = match_url
		)

		result = f.read()
		logging.info(str(result))
		js_ret = json.loads(result)
		if js_ret["status"] != "ok":
			return music_id

		if not js_ret.has_key("songs") or len(js_ret["songs"]) == 0:
			return music_id

		for song in js_ret["songs"]:
			if song["same_song"] == 1 and song["same_artist"] == 1 and song["same_album"] == 1 and song["same_version"] == 1:
				#if song["ar"] == "、".join(artistlist):
				music_id.append(int(song["id"]))
		return music_id
	except Exception,e:
		logging.error(str(e))
		pass

#取得文件的sig路径
def ImageFileProcess(sig1, sig2, path_pre, filename):
        Im = Image.open(filename)
        Im_format = Im.format
        if Im_format == "JPEG":
                Im_format = "jpg"
        elif Im_format == "PNG":
                Im_format = "png"
        elif Im_format == "BMP":
                Im_format = "bmp"
        else:
                Im_format = "jpg"
        #fileFormat = os.path.splitext(filename)[1].strip('.')
        fileFormat = Im_format
        sigPath = getSigPath(int(sig1), int(sig2), fileFormat)
        #print sigPath
        newPath = path_pre.rstrip('/') + sigPath
        #print newPath
        dirPath = os.path.dirname(newPath)
        #filename = unicode(filename.replace(" ","\ "),"utf8")
        #print filename
        if (not os.path.exists(dirPath)):
                os.makedirs(dirPath)

        shutil.copyfile(filename, newPath)
        os.remove(filename)
        return sigPath

#取得文件的sig路径
def songFileProcess(sig1, sig2, path_pre, filename):
	fileFormat = os.path.splitext(filename)[1].strip('.')
	sigPath = getSigPath(int(sig1), int(sig2), fileFormat)
	#print sigPath
	newPath = path_pre.rstrip('/') + sigPath
	#print newPath
	dirPath = os.path.dirname(newPath)

	if (not os.path.exists(dirPath)):
		os.makedirs(dirPath)

	shutil.copyfile(filename, newPath)
	#os.remove(filename)
	return sigPath

#sig路径计算
def getSigPath(sig1, sig2, format):
	d1 = sig2 % 100
	d2 = sig2 / 100 % 100
	dv = (sig1 ^ sig2) % 3 + 1
	sigpath = 's' + str(dv) + '/' + str(d1) + '/' + str(d2) + '/' + str(sig1) + '.' + str(format)
	return getAbsPath(sigpath)

def mksig(filename):
	f = open(filename, "r")
	file_content = f.read()
	f.close()
	md5hash = hashlib.md5(file_content)
	md5str = md5hash.hexdigest()
	md5list = list(struct.unpack('BBBBBBBBBBBBBBBB', md5str.decode('hex')))
	l = len(md5list)
	i = 0
	while i < l:
		md5list[i],md5list[i+3] = md5list[i+3],md5list[i]
		md5list[i+1],md5list[i+2] = md5list[i+2],md5list[i+1]
		i = i + 4
	md5hex = []
	for t in md5list:
		md5hex.append(hex(t).replace("0x","").zfill(2))
	hexstr = ""
	for t in md5hex:
		hexstr += t
	#print (filename)
	#print (hexstr)
	n1,n2,n3,n4 = list(struct.unpack('IIII', hexstr.decode('hex')))
	return n1^n2,n3^n4

def getAbsPath(path):
	dirs = path.split('/')

	absdirs = []
	for dir in dirs:
		if (dir == '.' or dir == ''):
			continue
		if (dir == '..'):
			if (len(absdirs) > 0):
				absdirs.pop()
			continue
		absdirs.append(dir)

	path = '/'
	for dir in absdirs:
		path = path + dir + '/'

	path = '/' + path.strip('/')
	return path

#下载文件
def download_url(file, url):
    #if os.path.exists(file):
    #    return 0

    cmd    = "wget --limit-rate=%dk --tries=3  --read-timeout=300 -q -O \"%s\" \"%s\" " % (5000, file, url)
    res    = os.system(cmd)
    if res != 0:
        cmd    = "wget --limit-rate=%dk --tries=2  --read-timeout=500 -q -O \"%s\" \"%s\" " % (5000, file, url)
        res    = os.system(cmd)
    else:
        res = 0
    return res

def loadConfig(path):
	fieldConfig = {}
	currentField = ""
	f = open(path,"r")
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

###
###2017-03-24 20:30:00~2017-03-25 00:15:00
###
def getTimeSpare(config_file):
        NoTaskTime = []
        fconfig = open(config_file,"r")
        for line in fconfig:
                if line.strip().find("#") == 0:
                        continue
                arr = line.strip().split("~")
                if len(arr) < 2:
                        continue
                beginTime = time.strptime(arr[0],"%Y-%m-%d %H:%M:%S")
                endTime = time.strptime(arr[1],"%Y-%m-%d %H:%M:%S")
                NoTaskTime.append((beginTime,endTime))
        fconfig.close()
        return NoTaskTime

def checkTimeSpare(config_file,dlog):
        NoTaskTime = getTimeSpare(config_file)
        timeNow = time.localtime()
        dlog.info(time.strftime("%Y-%m-%d %H:%M:%S",timeNow))
        timeSpareStatus = True
        while timeSpareStatus:
                flag = False
                NoTaskTime = getTimeSpare(config_file)
                for (beginTime,endTime) in NoTaskTime:
                        #print beginTime > endTime
                        dlog.info("begin time is " + time.strftime("%Y-%m-%d %H:%M:%S",beginTime))
                        dlog.info("end time is " + time.strftime("%Y-%m-%d %H:%M:%S",endTime))
                        if(timeNow > beginTime and timeNow < endTime):
                                dlog.info("No task time sleep 10s")
                                dlog.info(time.strftime("%Y-%m-%d %H:%M:%S",timeNow))
                                dlog.info("begin time is " + time.strftime("%Y-%m-%d %H:%M:%S",beginTime))
                                dlog.info("end time is " + time.strftime("%Y-%m-%d %H:%M:%S",endTime))
                                time.sleep(5)
                                timeNow = time.localtime()
                                flag = True
                timeSpareStatus = flag

def artist_name_process(path,output):
	artist = set()
        f = open(path,"r")
        for line in f:
                arr = line.strip().split("/")
                for a in arr:
			artist.add(a.strip())
        f.close()
	fout = open(output,"w+")
	for art in artist:
		fout.write("%s\n" % (art))
	fout.close()

def get_num_task_running(editor_id,conn,cur):
        count = 0
        try:
                sql = "select count(*) from DMSTask.Task where status != \"fail\" and status != \"success\" and editor_id = %s" % editor_id
                cnt = cur.execute(sql)
		logging.info(sql)
                conn.commit()
                if cnt > 0:
                        ret = cur.fetchone()
                        count = ret[0]
        except Exception,e:
                traceback.print_exc()
        return count

def get_kw_albumid(curRes,tm_album,kw_artist):
    kw_albumid = []
    try:
        album = MySQLdb.escape_string(tm_album)
        artist = MySQLdb.escape_string(kw_artist)
        sql = (''' select id from Album where name = "%s" and artist = "%s" ''') % (album,artist)
        cnt = curRes.execute(sql)
	logging.info(sql)
        if cnt > 0:
            rets = curRes.fetchall()
	    for ret in rets:
            	kw_albumid.append(ret[0])
    except:
        traceback.print_exc()
    return kw_albumid

def getArtistIdsFromAlbumIds(albumId,connRun,curRun):
        ids = set()
        sqlcmd = '''select version from ArtistMap where map_type=2 and from_id=%s order by version desc limit 1''' % (albumId)
        cnt = curRun.execute(sqlcmd)
	#logging.info(sqlcmd)
        if cnt > 0:
                ret = curRun.fetchone()
                version = ret[0]
                sqlcmd = '''select artist_id from ArtistMap where map_type=2 and from_id=%s and version=%s''' % (albumId,version)
                cnt = curRun.execute(sqlcmd)
		#logging.info(sqlcmd)
                if cnt > 0:
                        ret = curRun.fetchall()
                        for r in ret:
                                ids.add(r[0])
        return ids

def getNameSetfromArtistIds(artistIdset,connRun,curRun):
        nameSet = set()
        for artistId in artistIdset:
                sqlcmd = '''select m_name from Artist where id=%s''' % (artistId)
                cnt = curRun.execute(sqlcmd)
		#logging.info(sqlcmd)
                if cnt > 0:
                        ret = curRun.fetchone()
                        nameSet.add(ret[0].strip().lower())
        return nameSet

def getArtistIdMap(artistid,conn,cur):
	mapid = []
	sql = '''select right_arids from AutoDMS.ArtistRelation where wrong_arid=%s''' % (artistid)
	cnt = cur.execute(sql)
	if cnt > 0:
		ret = cur.fetchone()
		mapid = ret["right_arids"].split(";")
	return mapid

def getArtistIdsFromName(artists,split,connRun,curRun,connSrc,curSrc):
        Ids = set()
	art_arr = artists.strip().split(split)
	for art in art_arr:
		#artist = art.strip().replace("（","(").replace("）",")")
		artist = art.strip()
		#artist = getTrimMsg(artist)
		#if len(artist) > 96:
		#	artist = artist[:96]
                sqlcmd = '''select id from Artist where c_show_type !=16 and m_name=\"%s\" order by id''' % (MySQLdb.escape_string(artist))
                cnt = curRun.execute(sqlcmd)
		logging.info(sqlcmd)
                if cnt > 0:
                        ret = curRun.fetchone()
                        #Ids.add(str(ret[0]))
			mapid = getArtistIdMap(ret[0],connSrc,curSrc)
			if len(mapid) > 0:
				for id in mapid:
					Ids.add(str(id))
			else:
				Ids.add(str(ret[0]))
		else:
			sqlcmd = '''select id from Artist where (m_name=\"%s\" or m_name1=\"%s\" or m_name2=\"%s\" or m_name3=\"%s\" or m_name4=\"%s\" or m_name5=\"%s\") order by id''' % (MySQLdb.escape_string(artist),MySQLdb.escape_string(artist),MySQLdb.escape_string(artist),MySQLdb.escape_string(artist),MySQLdb.escape_string(artist),MySQLdb.escape_string(artist))
			cnt = curRun.execute(sqlcmd)
			logging.info(sqlcmd)
			if cnt > 0:
				ret = curRun.fetchone()
				#Ids.add(str(ret[0]))
				mapid = getArtistIdMap(ret[0],connSrc,curSrc)
				if len(mapid) > 0:
					for id in mapid:
						Ids.add(str(id))
				else:
					Ids.add(str(ret[0]))
        return list(Ids)

def getArtistIdFromName(artist,connRun,curRun,connSrc,curSrc):
	artistid = 0
	artist = artist.strip()
	sqlcmd = '''select id from Artist where c_show_type !=16 and m_name=\"%s\" order by id''' % (MySQLdb.escape_string(artist))
	cnt = curRun.execute(sqlcmd)
	logging.info(sqlcmd)
	if cnt > 0:
		ret = curRun.fetchone()
		mapid = getArtistIdMap(ret[0],connSrc,curSrc)
		if len(mapid) > 0:
			artistid = mapid[0]
		else:
			artistid = ret[0]
	else:
		sqlcmd = '''select id from Artist where (m_name=\"%s\" or m_name1=\"%s\" or m_name2=\"%s\" or m_name3=\"%s\" or m_name4=\"%s\" or m_name5=\"%s\") order by id''' % (MySQLdb.escape_string(artist),MySQLdb.escape_string(artist),MySQLdb.escape_string(artist),MySQLdb.escape_string(artist),MySQLdb.escape_string(artist),MySQLdb.escape_string(artist))
		cnt = curRun.execute(sqlcmd)
		logging.info(sqlcmd)
		if cnt > 0:
			ret = curRun.fetchone()
			mapid = getArtistIdMap(ret[0],connSrc,curSrc)
			if len(mapid) > 0:
				artistid = mapid[0]
			else:
				artistid = ret[0]
        return artistid

###get all albumids of the album name
def getAlbumids(albumname,connRun,curRun):
        ids = {}
        sqlcmd = '''select id,tx_albumid from Album where m_name=\"%s\" order by c_show_type,id''' % (MySQLdb.escape_string(albumname.strip()))
        #print sqlcmd
        cnt = curRun.execute(sqlcmd)
	logging.info(sqlcmd)
        if cnt > 0:
                ret = curRun.fetchall()
                for r in ret:
                        ids[r[0]] = r[1].strip()
        return ids

def checkAlbumEditing(kw_albumid,connRun,curRun):
	albumid = 0
	sql = '''select id from Album where id=%s and version_pub = version_editor''' % (kw_albumid)
	cnt = curRun.execute(sql)
	logging.info(sql)
        if cnt > 0:
		ret = curRun.fetchone()
		albumid = ret[0]
	return albumid
	
def checkAlbumExists(albumname,artists,connRun,curRun,connRes,curRes):
    ###find album id
    kw_ids = []
    ret_albumId = 0
    tx_albumid = ""
    logging.info("album match : %s, %s" % (albumname, artists))
    albumIds = getAlbumids(albumname,connRun,curRun)
    #print "albumids:",albumIds
    if len(albumIds) == 0:
        ###new album
	return kw_ids
        #return ret_albumId,tx_albumid
    else:
        ###album artist new
        artistset = set()
	if artists.find('/') != -1:
        	artistlist = artists.strip().split("/")
	elif artists.find('###') != -1:
        	artistlist = artists.strip().split("###")
	elif artists.find(';') != -1:
        	artistlist = artists.strip().split(";")
	elif artists.find('|') != -1:
        	artistlist = artists.strip().split("|")
	elif artists.find(',') != -1:
        	artistlist = artists.strip().split(",")
	elif artists.find('、') != -1:
        	artistlist = artists.strip().split("、")
	else:
        	artistlist = artists.strip().split("###")
        ###single artist
        if len(artistlist) <= 1:
            nAlbumIds = get_kw_albumid(curRes,albumname,artists)
	    kw_ids = nAlbumIds
	    return kw_ids
            #return nAlbumId,tx_albumid
        for a in artistlist:
            artistset.add(a.strip().lower())

        ###album artist in DB
        for albumId,tm_albumid in albumIds.items():
	    logging.info("matching album : %s" % (albumId))
            artistIdset = getArtistIdsFromAlbumIds(albumId,connRun,curRun)
            artistNames = getNameSetfromArtistIds(artistIdset,connRun,curRun)
            if artistset == artistNames:
		logging.info("matched : %s" % albumId)
		kw_ids.append(albumId)
                #return albumId,tx_albumid
    return kw_ids

def getMusicIds(albumid,musicname,connRes,curRes):
	ids = []
        sqlcmd = '''select id,allartistid,artistid,version,version2 from Music where albumid=%s and songname=\"%s\"''' % (albumid,MySQLdb.escape_string(musicname.strip()))
        #print sqlcmd
        cnt = curRes.execute(sqlcmd)
	logging.info(sqlcmd)
        if cnt > 0:
                ret = curRes.fetchall()
                for r in ret:
			if r[1].strip() != "":
                        	ids.append((r[0],r[1].strip(),r[3].strip(),r[4].strip()))
			else:
                        	ids.append((r[0],r[2],r[3].strip(),r[4].strip()))
        return ids

def getMusicIds2(albumid,musicname,connRes,curRes):
	ids = []
        sqlcmd = '''select id,artistid,version,version2 from Music where songname=\"%s\" and albumid=%s''' % (MySQLdb.escape_string(musicname.strip()),albumid)
        #print sqlcmd
        cnt = curRes.execute(sqlcmd)
	logging.info(sqlcmd)
        if cnt > 0:
                ret = curRes.fetchall()
                for r in ret:
                        ids.append((r[0],r[1],r[2].strip(),r[3].strip()))
        return ids

def getMusicFromid(mid,connRun,curRun):
	from_id = ""
	sql = '''select from_id from Music where id=%s''' % (mid)
	cnt = curRun.execute(sql)
	logging.info(sql)
	if cnt > 0:
		ret = curRun.fetchone()
		from_id = ret[0].strip()
		connRun.commit()
	return from_id

def checkMusicExists(albumid,musicname,artists,version,version2,connRun,curRun,connRes,curRes):
	ret_musicId = 0
	from_id = ""
	logging.info("music name match : %s,%s,%s" % (musicname, artists, version))
	listMusic = getMusicIds(albumid,musicname,connRes,curRes)
	art_src_set = set()
	if artists.find('###') != -1:
        	artistlist = artists.strip().split("###")
	elif artists.find('/') != -1:
        	artistlist = artists.strip().split("/")
	elif artists.find(';') != -1:
        	artistlist = artists.strip().split(";")
	elif artists.find('|') != -1:
        	artistlist = artists.strip().split("|")
	elif artists.find(',') != -1:
        	artistlist = artists.strip().split(",")
	elif artists.find('、') != -1:
        	artistlist = artists.strip().split("、")
	else:
        	artistlist = artists.strip().split("###")
        for a in artistlist:
            art_src_set.add(a.strip().lower())
	
	for item in listMusic:
		artset = set()
		mid = item[0]
		art_str = str(item[1]).strip()
		ver = item[2].strip()
		ver2 = item[3].strip()
		from_id = getMusicFromid(mid,connRun,curRun)
		logging.info("matching : %s , %s , %s , %s" % (mid, art_str, ver, from_id))
		if ver == "":
			ver = "完整版"
		if version == "":
			version = "完整版"
		for ar_id in art_str.split("&"):
			artset.add(ar_id.strip())
		artistNames = getNameSetfromArtistIds(artset,connRun,curRun)
		if art_src_set == artistNames and ver == version.strip() and ver2 == version2.strip():
			return mid,from_id
	return ret_musicId,from_id

def checkMusictm(tm_id,connRun,curRun):
	mid1 = 0
	mid2 = 0
	sql = '''select id from Music where from_id = \"tm_%s\" and version_editor != 1 and version_pub != 0''' % (tm_id)
	cnt1 = curRun.execute(sql)
	logging.info(sql)
	if cnt1 > 0:
		ret = curRun.fetchone()
		mid1 = ret[0]
	sql = '''select id from Music where from_id = \"tx_%s\" and version_editor != 1 and version_pub != 0''' % (tm_id)
	cnt2 = curRun.execute(sql)
	if cnt2 > 0:
		ret = curRun.fetchone()
		mid2 = ret[0]
	if cnt1 + cnt2 > 0:
		if mid2 != 0:
			return mid2
		return mid1
	return 0

def checkMusicRelation(tm_id,conn,cur):
	kw_id = 0
	replace_id = 0
	sql = '''select rid from KT_MusicRelation where mid=%s''' % (tm_id)
	cnt1 = cur.execute(sql)
	logging.info(sql)
	if cnt1 > 0:
		ret = cur.fetchone()
		kw_id= ret[0]
	'''
	if kw_id == 0:
		sql = "select replace_id from KT_TencentRepeat where mid=%s" % (tm_id)
		cnt1 = cur.execute(sql)
		logging.info(sql)
		if cnt1 > 0:
			ret = cur.fetchone()
			replace_id= ret[0]
	if replace_id > 0:
		sql = "select rid from KT_MusicRelation where mid=%s" % (replace_id)
		cnt1 = cur.execute(sql)
		logging.info(sql)
		if cnt1 > 0:
			ret = cur.fetchone()
			kw_id= ret[0]
	'''
	return kw_id

def checkMusicRelationKW(kw_id,conn,cur):
	tm_id = 0
	sql = '''select mid from KT_MusicRelation where rid=%s''' % (kw_id)
	cnt1 = cur.execute(sql)
	logging.info(sql)
	if cnt1 > 0:
		ret = cur.fetchone()
		tm_id= ret[0]
	return tm_id

def insert_KWRelation(kw_id,tx_id,level,conn,cur):
	try:
		tm_id = checkMusicRelationKW(kw_id,conn,cur)
		if tm_id == 0:
			sql = '''insert into KT_MusicRelation (rid,mid,level,ctime) values (%s,%s,%s,now())''' % (kw_id,tx_id,level)
			cnt = cur.execute(sql)
			if cnt > 0:
				conn.commit()
				logging.info("insert relation (%s,%s)" % (kw_id,tx_id))
			else:
				logging.info("insert relation failed (%s,%s)" % (kw_id,tx_id))
		return tm_id
	except Exception,e:
		logging.info(str(e))
		pass
		return 0

def insert_TencentRepeat(tx_id,replace_id,conn,cur):
	try:
		sql = '''insert into KT_TencentRepeat (mid,replace_id,ctime) values (%s,%s,now())''' % (tx_id,replace_id)
		cnt = cur.execute(sql)
		if cnt > 0:
			conn.commit()
			logging.info("insert tencent repeat (%s,%s)" % (tx_id,replace_id))
		else:
			logging.info("insert tencent repeat failed (%s,%s)" % (tx_id,replace_id))
	except Exception,e:
		logging.info(str(e))
		pass
		return 0

def checkAlbumRelation(tm_id,conn,cur):
	kw_ids = []
	replace_id = 0
	sql = '''select kw_albumid from KT_AlbumRelation where qq_albumid=%s''' % (tm_id)
	cnt1 = cur.execute(sql)
	logging.info(sql)
	if cnt1 > 0:
		rets = cur.fetchall()
		for ret in rets:
			kw_ids.append(ret[0])
	return kw_ids

def checkAlbumRelationKW(kw_id,conn,cur):
	qq_ids = []
	replace_id = 0
	sql = '''select qq_albumid from KT_AlbumRelation where kw_albumid=%s''' % (kw_id)
	cnt1 = cur.execute(sql)
	logging.info(sql)
	if cnt1 > 0:
		rets = cur.fetchall()
		for ret in rets:
			qq_ids.append(ret[0])
	return qq_ids

def insert_KWAlbumRelation(kw_id,tx_id,level,conn,cur):
	try:
		tm_ids = checkAlbumRelationKW(kw_id,conn,cur)
		if len(tm_ids) == 0:
			sql = '''insert into KT_AlbumRelation (kw_albumid,qq_albumid,level,ctime) values (%s,%s,%s,now())''' % (kw_id,tx_id,level)
			cnt = cur.execute(sql)
			if cnt > 0:
				conn.commit()
				logging.info("insert relation (%s,%s)" % (kw_id,tx_id))
			else:
				logging.info("insert relation failed (%s,%s)" % (kw_id,tx_id))
		return tm_ids
	except Exception,e:
		logging.error(str(e))
		pass
		return 0

def checkArtistRelation(tm_id,conn,cur):
	kw_ids = []
	sql = '''select kw_artistid from KT_ArtistRelation where qq_artistid=%s''' % (tm_id)
	cnt = cur.execute(sql)
	if cnt > 0:
		rets = cur.fetchall()
		for ret in rets:
			kw_ids.append(ret[0])
	return kw_ids

def checkArtistRelationKW(kw_id,conn,cur):
	qq_ids = []
	sql = '''select qq_artistid from KT_ArtistRelation where kw_artistid=%s''' % (kw_id)
	cnt = cur.execute(sql)
	if cnt > 0:
		rets = cur.fetchall()
		for ret in rets:
			qq_ids.append(ret[0])
	return qq_ids

def insert_KWArtistRelation(kw_id,tx_id,level,conn,cur):
	try:
		tm_ids = checkArtistRelationKW(kw_id,conn,cur)
		if len(tm_ids) == 0:
			sql = '''insert into KT_ArtistRelation (kw_artistid,qq_artistid,level,ctime) values (%s,%s,%s,now())''' % (kw_id,tx_id,level)
			cnt = cur.execute(sql)
			if cnt > 0:
				conn.commit()
				logging.info("insert relation (%s,%s)" % (kw_id,tx_id))
			else:
				logging.info("insert relation failed (%s,%s)" % (kw_id,tx_id))
		return tm_ids
	except Exception,e:
		logging.error(str(e))
		pass
		return 0

def get_kw_artists_tme(tme_singerids,split,conn,cur):
        ids = []
        names = []
        art_arr = tme_singerids.strip().split(split)
        for art in art_arr:
		if art.strip() == "":
			continue
                sql = '''select m_artist_id,m_name from ArtistSrc where tmeid=%s''' % art
		logging.info(sql)
                cnt = cur.execute(sql)
                if cnt > 0:
                        ret = cur.fetchone()
                        conn.commit()
                        if ret["m_artist_id"] == 0:
                                continue
                        ids.append(str(ret["m_artist_id"]))
                        names.append(str(ret["m_name"]))
        return ids,names

def checkAlbumtm(tm_id,connRun,curRun):
	mid1 = 0
	mid2 = 0
	sql = '''select id from Album where tx_albumid = \"tx_%s\" and version_editor != 1 order by id''' % (tm_id)
	cnt1 = curRun.execute(sql)
	logging.info(sql)
	if cnt1 > 0:
		ret = curRun.fetchone()
		mid1 = ret[0]
		return mid1,0
	sql = '''select id from Album where tx_albumid = \"tm_%s\" and version_editor != 1 order by id''' % (tm_id)
	cnt2 = curRun.execute(sql)
	logging.info(sql)
	if cnt2 > 0:
		ret = curRun.fetchone()
		mid2 = ret[0]
		return mid2,1
	return 0,1

def checkEditorAlbum(albumid,connRun,curRun):
	try:
		editor_ids = set([228,8,6,28,12,38,29,26,25,20,65,158,3,262,60,242,21,97])
		aid = 0
		is_editor = 0
		album_name = ""
		sql = '''select tx_albumid,m_name from Album where id = %s''' % (albumid)
		cnt = curRun.execute(sql)
		logging.info(sql)
		if cnt > 0:
			ret = curRun.fetchone()
			tx_albumid = ret[0].strip()
			album_name = ret[1].strip()
			if tx_albumid.find("tx_") != -1:
				is_editor = 1
				aid = int(tx_albumid.replace("tx_","").strip())
			if tx_albumid.find("tm_") != -1:
				is_editor = 0
				aid = int(tx_albumid.replace("tm_","").strip())
		if is_editor == 0:
			sql = '''select c_create_editor from Album where id = %s''' % (albumid)
			cnt = curRun.execute(sql)
			logging.info(sql)
			if cnt > 0:
				ret = curRun.fetchone()
				editor_id = ret[0]
				if editor_id in editor_ids:
					is_editor = 1
		if is_editor == 0:
			sql = '''select distinct(c_create_editor) from Music where m_album_id = %s''' % (albumid)
			cnt = curRun.execute(sql)
			logging.info(sql)
			if cnt > 0:
				ret = curRun.fetchall()
				for r in ret:
					editor_id = r[0]
					if editor_id in editor_ids:
						is_editor = 1
						break
		return aid,is_editor,album_name
	except Exception,e:
		logging.error(str(e))
		#pass

def checkMusicLyrics(rid,connRun,curRun):
	try:
		m_lrc = 0
		m_lrcx = 0
		m_lrcs = 0
		sql = '''select AudioSource.m_lrc,AudioSource.m_lrcx,AudioSource.m_lrcs from Music,AudioSource where Music.m_audio_id=AudioSource.id and Music.id=%s''' % (rid)
		cnt = curRun.execute(sql)
		if cnt > 0:
			ret = curRun.fetchone()
			m_lrc = ret[0]
			m_lrcx = ret[1]
			m_lrcs = ret[2]
		return m_lrc,m_lrcx,m_lrcs
	except Exception,e:
		logging.error(str(e))
		return 0,0,0
	
def checkMusicLyricsEditor(rid,connRun,curRun):
	try:
		m_lrc = 0
		m_lrcx = 0
		m_lrcs = 0
		editor_id = 0
		sql = '''select AudioSource.m_lrc,AudioSource.m_lrcx,AudioSource.m_lrcs,AudioSource.c_modify_editor from Music,AudioSource where Music.m_audio_id=AudioSource.id and Music.id=%s''' % (rid)
		cnt = curRun.execute(sql)
		if cnt > 0:
			ret = curRun.fetchone()
			m_lrc = ret[0]
			m_lrcx = ret[1]
			m_lrcs = ret[2]
			editor_id = ret[3]
		return m_lrc,m_lrcx,m_lrcs,editor_id
	except Exception,e:
		logging.error(str(e))
		return 0,0,0,0

def checkEditor(rid,table,connRun,curRun):
	try:
		editor_id = 0
		sql = '''select c_modify_editor from %s where id=%s''' % (table,rid)
		cnt = curRun.execute(sql)
		if cnt > 0:
			ret = curRun.fetchone()
			editor_id = ret[0]
		return editor_id
	except Exception,e:
		logging.error(str(e))
		return 0

def checkMusicAudio(rid,connRun,curRun):
	try:
		m_audio_id = 0
		sql = '''select m_audio_id from Music where id=%s''' % (rid)
		cnt = curRun.execute(sql)
		if cnt > 0:
			ret = curRun.fetchone()
			m_audio_id = ret[0]
		return m_audio_id
	except Exception,e:
		logging.error(str(e))
		return 0
