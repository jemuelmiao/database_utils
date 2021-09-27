# -*- coding: utf-8 -*-
'''
	@author: 	jemuelmiao
	@filename: 	iexporter.py
	@create: 	2018-11-03 22:27:17
	@modify:	2018-11-05 22:20:45
	@desc:		
'''
import const
import config
from sqlparse import SqlParser
import os
import codecs

class BaseConverter(object):
	def __init__(self, mongo, dbMode, dbNames):
		self.mongo = mongo
		self.dbMode = dbMode
		self.dbNames = dbNames
		self.tableMap = {}

	def getTableName(self, dbName, colName):
		return '%s_%s'%(dbName, colName)

	def getFileName(self, dbName, colName):
		return '%s_%s.sql'%(dbName, colName)

	def getSortTables(self):
		orgTableList = []
		refTableMap = {}
		ignoreTables = config.getIgnoreTables()
		for dbName in self.mongo.getDbNames():
			if self.dbMode == const.DB_EXCLUDE:
				if dbName in self.dbNames:
					continue
			else:
				if dbName not in self.dbNames:
					continue
			for colName in self.mongo.getColNames(dbName):
				tableName = self.getTableName(dbName, colName)
				if tableName in ignoreTables:
					continue
				orgTableList.append(tableName)
				self.tableMap[tableName] = (dbName, colName)
				_, refTables = config.getForeigns(dbName, colName)
				if refTables is not None:
					refTableMap[tableName] = refTables
		sortTableList = []
		# 要求不能出现循环引用
		while True:
			if len(orgTableList) == len(sortTableList):
				break
			for tableName in orgTableList:
				if tableName in sortTableList:
					continue
				if tableName in refTableMap:
					continue
				sortTableList.append(tableName)
				delList = []
				for refTableName, refTables in refTableMap.iteritems():
					if tableName in refTables:
						refTables.remove(tableName)
					if len(refTables) == 0:
						delList.append(refTableName)
				for name in delList:
					refTableMap.pop(name)
		return sortTableList

class Exporter(BaseConverter):
	def run(self):
		sqlType = config.getSqlType()
		sqlDir = config.getSqlDir()
		sortTableList = self.getSortTables()
		addBatch = []
		delBatch = []
		for tableName in sortTableList:
			dbName, colName = self.tableMap[tableName]
			col = self.mongo.getCol(dbName, colName)
			sqlParser = SqlParser()
			dataLimit = config.getDataLimit(dbName, colName)
			cursor = col.find({}, no_cursor_timeout=True).limit(dataLimit)
			for doc in cursor:
				sqlParser.parse(doc, '')
			cursor.close()
			fieldMode, fields = config.getFields(dbName, colName)
			if fieldMode == const.FIELD_INCLUDE:
				sqlParser.remain(fields)
			elif fieldMode == const.FIELD_EXCLUDE:
				sqlParser.remove(fields)
			fileName = self.getFileName(dbName, colName)
			primary = config.getPrimary(dbName, colName)
			uniques = config.getUniques(dbName, colName)
			foreigns, _ = config.getForeigns(dbName, colName)
			sql = sqlParser.getCreateSql(tableName, primary, uniques, foreigns)
			try:
				path = os.path.join(sqlDir, fileName)
				with codecs.open(path, 'w', encoding='utf-8') as fp:
					fp.write(sql)
				if sqlType == const.TYPE_MYSQL:
					addBatch.append('source %s'%(os.path.realpath(path)))
				elif sqlType == const.TYPE_PGSQL:
					addBatch.append(sql)
				delBatch.insert(0, 'drop table %s;'%(tableName))
				print 'succ: %s %s %s'%(dbName, colName, fileName)
			except Exception, e:
				print 'fail: %s %s %s'%(dbName, colName, fileName), e
		with codecs.open(os.path.join(sqlDir, config.getAddBatchFile()), 'w', encoding='utf-8') as fp:
			fp.write('\n'.join(addBatch))
		with codecs.open(os.path.join(sqlDir, config.getDelBatchFile()), 'w', encoding='utf-8') as fp:
			fp.write('\n'.join(delBatch))

class Importer(BaseConverter):
	def run(self, sqlClient):
		sortTableList = self.getSortTables()
		for tableName in sortTableList:
			dbName, colName = self.tableMap[tableName]
			col = self.mongo.getCol(dbName, colName)
			dataLimit = config.getDataLimit(dbName, colName)
			cursor = col.find({}, no_cursor_timeout=True)
			i = 0
			for doc in cursor:
				if i >= dataLimit:
					break
				sqlParser = SqlParser()
				sqlParser.parse(doc, '')
				fieldMode, fields = config.getFields(dbName, colName)
				if fieldMode == const.FIELD_INCLUDE:
					sqlParser.remain(fields)
				elif fieldMode == const.FIELD_EXCLUDE:
					sqlParser.remove(fields)
				sql = sqlParser.getInsertSql(tableName)
				#print sql.encode('utf-8')
				try:
					sqlClient.execute(sql)
					i += 1
					print 'succ: %s %s %s'%(dbName, colName, doc.get('_id', ''))
				except Exception, e:
					try:
						print 'fail: %s %s %s'%(dbName, colName, doc.get('_id', ''))#, e
					except:
						pass
			cursor.close()
