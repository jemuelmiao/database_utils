# -*- coding: utf-8 -*-
'''
	@author: 	jemuelmiao
	@filename: 	parser.py
	@create: 	2018-11-03 22:52:25
	@modify:	2018-11-07 15:07:26
	@desc:		
'''
from bson.int64 import Int64
from bson.objectid import ObjectId
import utils
import const
import config
import datetime
import time

class TypeObj(object):
	def __init__(self, tp, val):
		self.type = tp
		self.value = val


class SqlParser(object):
	def __init__(self):
		self.fieldInfo = {}
		self.sqlType = config.getSqlType()

	def parse(self, data, prefix):
		prefix = prefix.strip()
		if prefix.startswith('__'):
			return
		if type(data) == dict:
			for key, val in data.iteritems():
				if prefix == '':
					self.parse(val, key)
				else:
					self.parse(val, prefix+'.'+key)
		elif type(data) == list:
			for i, val in enumerate(data):
				if i >= config.getListDepth():
					break
				if prefix == '':
					self.parse(val, str(i))
				else:
					self.parse(val, '%s.%d'%(prefix, i))
		elif type(data) == str or type(data) == unicode:
			self.addStr(prefix, data)
		elif type(data) in (int, long):
			self.addInt(prefix, data)
		elif type(data) == float:
			if data == int(data):
				self.addInt(prefix, data)
			else:
				self.add(prefix, const.TYPE_DOUBLE[self.sqlType], data)
		elif type(data) == bool:
			if data:
				self.add(prefix, const.TYPE_TINYINT[self.sqlType], 1)
			else:
				self.add(prefix, const.TYPE_TINYINT[self.sqlType], 0)
		elif type(data) == Int64:
			self.addInt(prefix, data)
		elif type(data) == ObjectId:
			self.add(prefix, const.TYPE_VARCHAR_100[self.sqlType], str(data))
		elif type(data) == datetime.datetime:
			self.addInt(prefix, int(time.mktime(data.timetuple())))
		elif type(data) == type(None):
			pass
		else:
			print prefix, type(data)

	def add(self, field, tp, val):
		self.fieldInfo[field] = TypeObj(tp, val)

	def remove(self, removeFields):
		fieldList = self.fieldInfo.keys()
		for field in removeFields:
			for key in fieldList:
				if key == field or key.startswith(field+'.'):
					self.fieldInfo.pop(key)

	def remain(self, remainFields):
		fieldList = self.fieldInfo.keys()
		for key in fieldList:
			if key in remainFields:
				continue
			isFind = False
			for field in remainFields:
				if key.startswith(field+'.'):
					isFind = True
					break
			if isFind:
				continue
			self.fieldInfo.pop(key)

	def addInt(self, field, val):
		st = self.sqlType
		if val >= 2147483647:
			if field not in self.fieldInfo or self.fieldInfo[field].type not in (const.TYPE_BIGINT[st], ):
				self.fieldInfo[field] = TypeObj(const.TYPE_BIGINT[st], int(val))
		elif val >= 127:
			if field not in self.fieldInfo or self.fieldInfo[field].type not in (const.TYPE_BIGINT[st], const.TYPE_INT[st]):
				self.fieldInfo[field] = TypeObj(const.TYPE_INT[st], int(val))
		else:
			if field not in self.fieldInfo or self.fieldInfo[field].type not in (const.TYPE_BIGINT[st], const.TYPE_INT[st], const.TYPE_TINYINT[st]):
				self.fieldInfo[field] = TypeObj(const.TYPE_TINYINT[st], int(val))

	def addStr(self, field, val):
		st = self.sqlType
		if len(val) >= 65535:
			if field not in self.fieldInfo or self.fieldInfo[field].type not in (const.TYPE_MEDIUMTEXT[st], ):
				self.fieldInfo[field] = TypeObj(const.TYPE_MEDIUMTEXT[st], val)
		elif len(val) >= 300:
			if field not in self.fieldInfo or self.fieldInfo[field].type not in (const.TYPE_MEDIUMTEXT[st], const.TYPE_TEXT[st], ):
				self.fieldInfo[field] = TypeObj(const.TYPE_TEXT[st], val)
		elif len(val) >= 100:
			if field not in self.fieldInfo or self.fieldInfo[field].type not in (const.TYPE_MEDIUMTEXT[st], const.TYPE_TEXT[st], const.TYPE_VARCHAR_300[st]):
				self.fieldInfo[field] = TypeObj(const.TYPE_VARCHAR_300[st], val)
		elif len(val) >= 50:
			if field not in self.fieldInfo or self.fieldInfo[field].type not in (const.TYPE_MEDIUMTEXT[st], const.TYPE_TEXT[st], const.TYPE_VARCHAR_300[st], const.TYPE_VARCHAR_100[st]):
				self.fieldInfo[field] = TypeObj(const.TYPE_VARCHAR_100[st], val)
		elif len(val) >= 10:
			if field not in self.fieldInfo or self.fieldInfo[field].type not in (const.TYPE_MEDIUMTEXT[st], const.TYPE_TEXT[st], const.TYPE_VARCHAR_300[st], const.TYPE_VARCHAR_100[st], const.TYPE_VARCHAR_50[st]):
				self.fieldInfo[field] = TypeObj(const.TYPE_VARCHAR_50[st], val)
		else:
			if field not in self.fieldInfo or self.fieldInfo[field].type not in (const.TYPE_MEDIUMTEXT[st], const.TYPE_TEXT[st], const.TYPE_VARCHAR_300[st], const.TYPE_VARCHAR_100[st], const.TYPE_VARCHAR_50[st], const.TYPE_VARCHAR_10[st]):
				self.fieldInfo[field] = TypeObj(const.TYPE_VARCHAR_10[st], val)

	def getCreateSql(self, tableName, primary, uniques, foreigns):
		quote = config.getQuote()
		fields = []
		for field, typeObj in self.fieldInfo.iteritems():
			fields.append('\t%s%s%s %s,'%(quote, field, quote, typeObj.type))
		content = ''
		if len(fields) > 0:
			content += '\n'.join(fields)[:-1]
		if len(primary) > 0:
			if len(content) > 0:
				content += ',\n'
			content += '%s'%(primary)
		if len(uniques) > 0:
			if len(content) > 0:
				content += ',\n'
			content += '%s'%(uniques)
		if len(foreigns) > 0:
			if len(content) > 0:
				content += ',\n'
			content += '%s'%(foreigns)
		sql = '''
drop table if exists %s%s%s;
create table %s%s%s (
%s
)'''%(quote, tableName, quote, quote, tableName, quote, content)
		if self.sqlType == const.TYPE_MYSQL:
			sql += ' engine=InnoDB default charset=utf8mb4;'
		elif self.sqlType == const.TYPE_PGSQL:
			sql += ';'
		return sql

	def getInsertSql(self, tableName):
		st = self.sqlType
		quote = config.getQuote()
		fields = []
		values = []
		for field, typeObj in self.fieldInfo.iteritems():
			fields.append('%s%s%s'%(quote, field, quote))
			if typeObj.type in (const.TYPE_VARCHAR_10[st], const.TYPE_VARCHAR_50[st], const.TYPE_VARCHAR_100[st], const.TYPE_VARCHAR_300[st], const.TYPE_TEXT[st], const.TYPE_MEDIUMTEXT[st]):
				if self.sqlType == const.TYPE_MYSQL:
					values.append('"%s"'%(typeObj.value.replace('\\', '\\\\').replace('"', '\\"')))
				elif self.sqlType == const.TYPE_PGSQL:
					values.append("'%s'"%(typeObj.value.replace('\\', '\\\\').replace('"', '\\"')))
				else:
					values.append('%s'%(typeObj.value.replace('\\', '\\\\').replace('"', '\\"')))
			else:
				values.append(str(typeObj.value))
		sql = 'insert into %s%s%s (%s) values (%s);'%(quote, tableName, quote, ','.join(fields), ','.join(values))
		return sql