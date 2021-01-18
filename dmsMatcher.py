#!/bin/python
#coding=utf-8
from logger import *
import MySQLdb
import traceback

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

def getArtistIdFromName(artist,loginid,connRun,curRun):
	try:
		artistid = 0
		artist = artist.strip()
		sqlcmd = '''select id from Artist where c_show_type !=16 and m_name=\"%s\" and artist_loginid = \"%s\" and version_editor>1 order by id''' % (MySQLdb.escape_string(artist),loginid)
		cnt = curRun.execute(sqlcmd)
		logging.info(sqlcmd)
		if cnt > 0:
			ret = curRun.fetchone()
			artistid = ret[0]
		else:
			sqlcmd = '''select id from Artist where (m_name=\"%s\" or m_name1=\"%s\" or m_name2=\"%s\" or m_name3=\"%s\" or m_name4=\"%s\" or m_name5=\"%s\") and artist_loginid = \"%s\" and version_editor>1 order by id''' % (MySQLdb.escape_string(artist),MySQLdb.escape_string(artist),MySQLdb.escape_string(artist),MySQLdb.escape_string(artist),MySQLdb.escape_string(artist),MySQLdb.escape_string(artist),loginid)
			cnt = curRun.execute(sqlcmd)
			logging.info(sqlcmd)
			if cnt > 0:
				ret = curRun.fetchone()
				artistid = ret[0]
		return artistid
	except Exception,e:
		logging.error(str(e))
		pass


def getAlbumIdsRuntime(name,connRun,curRun):
	try:
		ids = {}
		sql = '''select id,version_editor,basic_company,company_id,c_show_type from DMSRuntime.Album where m_name="%s" and version_editor!=1 and c_show_type!=2''' % (MySQLdb.escape_string(name.strip()))
		cnt = curRun.execute(sql)
		if cnt > 0:
			rets = curRun.fetchall()
			for ret in rets:
				ids[ret[0]] = {"id":ret[0],"version_editor":ret[1],"basic_company":ret[2],"company_id":ret[3],"c_show_type":ret[4]}
		return ids
	except Exception,e:
		logging.error(str(e))
		pass

def matchAlbumSql(name,artistids,connRun,curRun):
	try:
		#resource or dmsruntime
		matched_ids_maybe = []
		#resource or dmsruntime
		#1.check music by albumid and name---musiclist
		albumlist = getAlbumIdsRuntime(name,connRun,curRun)
		for album_id,album in albumlist.items():
			#check artists
			kw_artistids = getArtistIdsFromAlbumIds(album_id,album["version_editor"],connRun,curRun)
			if set(artistids) != set(kw_artistids):
				logging.info("album_id %s artist not match" % (album_id))
				continue
			logging.info("album_id %s artist match" % (album_id))
			matched_ids_maybe.append(album_id)
		return matched_ids_maybe
	except Exception,e:
		logging.error(str(e))
		traceback.print_exc()
		pass

def getMusicIdsRuntime(name,version,albumid,connRun,curRun):
	try:
		ids = {}
		if version == "" or version == "完整版":
			basic_version = ""
			sql = '''select id,version_editor,basic_company,c_show_type from DMSRuntime.Music where m_name="%s" and m_album_id=%s and basic_version="%s" and version_editor!=1 and c_show_type !=2''' % (MySQLdb.escape_string(name.strip()),albumid,MySQLdb.escape_string(basic_version.strip()))
			logging.info(sql)
			cnt = curRun.execute(sql)
			if cnt > 0:
				rets = curRun.fetchall()
				for ret in rets:
					#ids[r["id"]] = r
					ids[ret[0]] = {"id":ret[0],"version_editor":ret[1],"basic_company":ret[2],"c_show_type":ret[3]}
			basic_version = "完整版"
			sql = '''select id,version_editor,basic_company,c_show_type from DMSRuntime.Music where m_name="%s" and m_album_id=%s and basic_version="%s" and version_editor!=1''' % (MySQLdb.escape_string(name.strip()),albumid,MySQLdb.escape_string(basic_version.strip()))
			logging.info(sql)
			cnt = curRun.execute(sql)
			if cnt > 0:
				rets = curRun.fetchall()
				for ret in rets:
					#ids[r["id"]] = r
					ids[ret[0]] = {"id":ret[0],"version_editor":ret[1],"basic_company":ret[2],"c_show_type":ret[3]}
		else:
			sql = '''select id,version_editor,basic_company,c_show_type from DMSRuntime.Music where m_name="%s" and m_album_id=%s and basic_version="%s" and version_editor!=1''' % (MySQLdb.escape_string(name.strip()),albumid,MySQLdb.escape_string(version.strip()))
			logging.info(sql)
			cnt = curRun.execute(sql)
			if cnt > 0:
				rets = curRun.fetchall()
				for ret in rets:
					#ids[r["id"]] = r
					ids[ret[0]] = {"id":ret[0],"version_editor":ret[1],"basic_company":ret[2],"c_show_type":ret[3]}
		return ids
	except Exception,e:
		logging.error(str(e))
		traceback.print_exc()
		pass

def getArtistIdsFromAlbumIds(albumId,version,connRun,curRun):
        ids = set()
	sqlcmd = '''select artist_id from ArtistMap where map_type=2 and from_id=%s and version=%s''' % (albumId,version)
	cnt = curRun.execute(sqlcmd)
	#logging.info(sqlcmd)
	if cnt > 0:
		ret = curRun.fetchall()
		for r in ret:
			ids.add(r[0])
        return ids

def getArtistIdsFromMusicIds(kw_id,version,connRun,curRun):
        ids = set()
	sqlcmd = '''select artist_id from ArtistMap where map_type=1 and from_id=%s and version=%s''' % (kw_id,version)
	cnt = curRun.execute(sqlcmd)
	#logging.info(sqlcmd)
	if cnt > 0:
		ret = curRun.fetchall()
		for r in ret:
			ids.add(r[0])
        return ids


###check base on artistid and albumid
###name string, albumid int, artistids [] list
def matchMusicSql(name,m_version,m_version2,albumid,artistids,connRun,curRun):
	try:
		matched_ids_maybe = []
		#resource or dmsruntime
		#1.check music by albumid and name---musiclist
		musiclist = getMusicIdsRuntime(name,m_version,albumid,connRun,curRun)
		for kw_id,info in musiclist.items():
			#check artists
			kw_artistids = getArtistIdsFromMusicIds(kw_id,info["version_editor"],connRun,curRun)
			if set(artistids) != set(kw_artistids):
				logging.info("kw_id %s artist not match" % (kw_id))
				continue
			logging.info("kw_id %s artist match" % (kw_id))
			matched_ids_maybe.append(kw_id)
		return matched_ids_maybe
	except Exception,e:
		logging.error(str(e))
		traceback.print_exc()
		pass

