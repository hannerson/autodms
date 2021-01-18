#coding=utf-8

import os,sys
import MySQLdb

class sqlClass(object):
	def __init__(self,conn,cur):
		self.conn = conn
		self.cur = cur

	def mysqlSelect(self,table,where,limit,param):
		#print table,limit,param
		s = "SELECT "
		s += "%s" % (",".join(param))
		s = s.rstrip(",")
		#print s
		s += " FROM " + table
		if where != "":
			s += " WHERE " + where
		s = s.lstrip()
		if limit > 0:
			s += " limit " + str(limit)
		print s
		result = []
		cnt = self.cur.execute(s)
		self.conn.commit()
		if cnt > 0:
			ret = self.cur.fetchall()
			for r in ret:
				r_dict = {}
				for i in range(len(r)):
					r_dict[param[i]] = r[i]
				result.append(r_dict)
			return result
		else:
			return []
			

	def mysqlInsert(self,table,param_dict):
		cols = ""
		vals = ""
		for k,v in param_dict.items():
			cols += "%s," % (k)
			print type(v)
			if type(v) == int:
				vals += "%s," % (v)
			elif type(v) == str or type(v) == unicode:
				vals += "\"%s\"," % (MySQLdb.escape_string(v))
		cols = cols.rstrip(",")
		vals = vals.rstrip(",")
		s = "INSERT INTO " + table + "("
		###colomns
		s += cols
		s += ")"
		s += " VALUES ("
		###values
		s += vals
		s += ")"
		print s
		cnt = 0
		lastid = 0
		cnt = self.cur.execute(s)
		if cnt > 0:
			self.conn.commit()
			lastid = int(self.cur.lastrowid)
		return lastid

	def mysqlInsertUpdate(self,table,param_dict,key):
		cols = ""
		vals = ""
		for k,v in param_dict.items():
			cols += "%s," % (k)
			if type(v) == int:
				vals += "%s," % (v)
			elif type(v) == str or type(v) == unicode:
				vals += "\"%s\"," % (MySQLdb.escape_string(v))
		cols = cols.rstrip(",")
		vals = vals.rstrip(",")
		s = "INSERT INTO " + table + "("
		###colomns
		s += cols
		s += ")"
		s += " VALUES ("
		###values
		s += vals
		s += ")"
		s += " ON DUPLICATE KEY UPDATE "
		for k,v in param_dict.items():
			if k in key:
				continue
			if type(v) == int:
				s += "%s=%s," % (k,v)
			elif type(v) == str or type(v) == unicode:
				s += "%s=\"%s\"," % (k,MySQLdb.escape_string(v))

		s = s.rstrip(",")
		print s
		cnt = 0
		lastid = 0
		cnt = self.cur.execute(s)
		if cnt > 0:
			self.conn.commit()
			lastid = int(self.cur.lastrowid)
		return lastid

	def mysqlUpdate(self,table,where,param_dict):
		s = "UPDATE " + table + " SET "
		for k,v in param_dict.items():
			if type(v) == int or type(v) == long:
				s += "%s=%s," % (k,v)
			elif type(v) == str or type(v) == unicode:
				s += "%s=\"%s\"," % (k,MySQLdb.escape_string(v))
		s = s.rstrip(",") + " WHERE " + where
		s = s.strip()
		print s
		cnt = self.cur.execute(s)
		if cnt > 0:
			self.conn.commit()
		return cnt

	def mysqlDelete(self,table,where):
		s = "DELETE FROM " + table + " WHERE " + where
		#print s
		cnt = self.cur.execute(s)
		if cnt > 0:
			self.conn.commit()
		return cnt
