# -*- coding: utf-8 -*-
'''
	@author: 	jemuelmiao
	@filename: 	mongo.py
	@create: 	2018-11-03 22:17:06
	@modify:	2018-11-05 13:33:28
	@desc:		
'''
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

class Mongo(object):
	def __init__(self, host, port, user, password):
		if user and password:
			h = 'mongodb://%s:%s@%s' % (user, password, host)
		else:
			h = 'mongodb://%s' % (host)
		self.conn = MongoClient(host=h, port=port)
		self.dbMap = self.getDbMap()

	def getDbMap(self):
		dbMap = {}
		for dbName in self.conn.database_names():
			dbMap[dbName] = []
			db = Database(self.conn, dbName)
			for colName in db.collection_names():
				if colName.startswith('__'):
					continue
				col = db.get_collection(colName)
				dbMap[dbName].append(col)
		return dbMap

	def getCol(self, dbName, colName):
		for col in self.dbMap.get(dbName, []):
			if col.name == colName:
				return col
		return None

	def getDbNames(self):
		return self.dbMap.keys()

	def getColNames(self, dbName):
		return [col.name for col in self.dbMap.get(dbName, [])]