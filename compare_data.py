# -*- coding: utf-8 -*-
'''
	@author: 	jemuelmiao
	@filename: 	compare_data.py
	@create: 	2018-06-11 20:19:09
	@modify:	2021-09-27 13:07:03
	@desc:		比较数据库
'''
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from optparse import OptionParser

# 字段比较模式，包含、排除
CMP_INCLUDE = 1
CMP_EXCLUDE = 2

class ColData(object):
	def __init__(self, data):
		super(ColData, self).__init__()
		self._data = data

	@property
	def data(self):
		return self._data

	def _get(self, data, keys):
		if len(keys) == 0:
			return data
		pos = keys.find('.')
		if pos == -1:
			key = keys
			remain = ''
		else:
			key = keys[:pos]
			remain = keys[pos+1:]
		if type(data) == dict and key in data:
			return self._get(data[key], remain)
		return None

	def __getitem__(self, keys):
		return self._get(self._data, keys)


class ColInfo(object):
	def __init__(self, name, dataList):
		super(ColInfo, self).__init__()
		self._name = name
		self._dataDict = {} #{_id: ColData}
		self.init(dataList)

	def init(self, dataList):
		for data in dataList:
			if '_id' in data:
				self._dataDict[data['_id']] = ColData(data)

	@property
	def name(self):
		return self._name

	@property
	def idList(self):
		return self._dataDict.keys()

	def __getitem__(self, id):
		if id in self._dataDict:
			return self._dataDict[id]
		return None		

class DBInfo(object):
	def __init__(self, name):
		super(DBInfo, self).__init__()
		self._name = name
		self._colDict = {} #{colname: ColInfo}

	def addCol(self, colInfo):
		self._colDict[colInfo.name] = colInfo

	def getCol(self, colName):
		if colName in self._colDict:
			return self._colDict[colName]
		return None

	@property
	def name(self):
		return self._name

class DataSource(object):
	def __init__(self, client, name):
		super(DataSource, self).__init__()
		self._client = client
		self._name = name
		self._dbDict = {} #{dbname: DBInfo}

	@property
	def name(self):
		return self._name

	def get(self, dbName, colName):
		if dbName not in self._dbDict:
			return None
		dbInfo = self._dbDict[dbName]
		colInfo = dbInfo.getCol(colName)
		return colInfo

	def load(self, dbName, colName):
		if dbName not in self._client.client.database_names():
			return None
		db = Database(self._client.client, dbName)
		if colName not in db.collection_names():
			return None
		col = db.get_collection(colName)
		dataList = []
		cursor = col.find()
		while True:
			try:
				data = cursor.next()
				dataList.append(data)
			except StopIteration:
				break
		colInfo = ColInfo(colName, dataList)
		if dbName not in self._dbDict:
			self._dbDict[dbName] = DBInfo(dbName)
		self._dbDict[dbName].addCol(colInfo)
		return colInfo

class Client(object):
	def __init__(self, host, port, user, password):
		super(Client, self).__init__()
		self._host = host
		self._port = port
		self._user = user
		self._password = password
		self._client = None
		self.init()

	def init(self):
		if self._user and self._password:
			host = 'mongodb://%s:%s@%s' % (self._user, self._password, self._host)
		else:
			host = 'mongodb://%s' % (self._host)
		self._client = MongoClient(host=host, port=self._port)

	@property
	def client(self):
		return self._client

	@property
	def host(self):
		return self._host

	@property
	def port(self):
		return self._port

	@property
	def user(self):
		return self._user

	@property
	def password(self):
		return self._password

class Comparer(object):
	def __init__(self, dataSource1, dataSource2, cmpMode, cmpFields, showFields):
		super(Comparer, self).__init__()
		self._cmpMode = cmpMode
		self._cmpFields = cmpFields
		self._showFields = showFields
		self._dataSource1 = dataSource1
		self._dataSource2 = dataSource2

	def run(self, dbName, colName):
		print '================Compare==============[%s.%s]' % (dbName, colName)
		colInfo1 = self._dataSource1.load(dbName, colName)
		colInfo2 = self._dataSource2.load(dbName, colName)
		if colInfo1 == None:
			print '[%s] is empty' % (self._dataSource1.name)
			return
		if colInfo2 == None:
			print '[%s] is empty' % (self._dataSource2.name)
			return
		idList1 = colInfo1.idList
		idList2 = colInfo2.idList
		commIds = set(idList1).intersection(idList2)
		allIds = set(idList1).union(idList2)
		diffIds = allIds.difference(commIds)
		diffIds1 = diffIds.intersection(idList1)
		diffIds2 = diffIds.intersection(idList2)
		print 'total id:%d' % (len(allIds))
		print '[%s] diff id:%d' % (self._dataSource1.name, len(diffIds1))
		for id in diffIds1:
			print '\t' + str(id),
			if len(self._showFields) > 0:
				print '[',
				colData = colInfo1[id]
				for f in self._showFields:
					print colData[f],
				print ']',
			print ''
		print '[%s] diff id:%d' % (self._dataSource2.name, len(diffIds2))
		for id in diffIds2:
			print '\t' + str(id),
			if len(self._showFields) > 0:
				print '[',
				colData = colInfo2[id]
				for f in self._showFields:
					print colData[f],
				print ']',
			print ''
		retList = []
		for id in commIds:
			data1 = colInfo1[id].data
			data2 = colInfo2[id].data
			ret = self.compare(data1, data2, self._cmpMode, self._cmpFields, '')
			if len(ret) > 0:
				# count += 1
				retList.append((id, ret))
		print 'diff data:%d' % (len(retList))
		for ret in retList:
			print '\t', ret[0], self.pt(ret[1])

	def compare(self, data1, data2, cmpMode, fieldList, base):
		if type(data1) != type(data2):
			return [base]
		if type(data1) == dict:
			diffList = []
			for k, v in data1.iteritems():
				key = k
				if base != '':
					key = '%s.%s' % (base, key)
				if k not in data2:
					if cmpMode == CMP_EXCLUDE:
						if key not in fieldList:
							diffList.append(key)
					else:
						if key in fieldList:
							diffList.append(key)
					continue
				diffs = self.compare(v, data2[k], cmpMode, fieldList, key)
				for d in diffs:
					if cmpMode == CMP_EXCLUDE:
						if d not in fieldList:
							diffList.append(d)
					else:
						if d in fieldList:
							diffList.append(d)
			for k, v in data2.iteritems():
				if k not in data1:
					key = k
					if base != '':
						key = '%s.%s' % (base, key)
					if cmpMode == CMP_EXCLUDE:
						if key not in fieldList:
							diffList.append(key)
					else:
						if key in fieldList:
							diffList.append(key)
			return diffList
		else:
			if data1 == data2:
				return []
			else:
				return [base]

	def pt(self, data):
		if type(data) == dict:
			tmp = {}
			for k, v in data.iteritems():
				tmp[self.pt(k)] = self.pt(v)
			return tmp
		elif type(data) == list:
			tmp = []
			for v in data:
				tmp.append(self.pt(v))
			return tmp
		elif type(data) == unicode:
			return str(data)
		else:
			return data

def GetHostInfo(info):
	user = ''
	password = ''
	ip = ''
	port = -1
	sp = info.strip().split('@')
	if len(sp) != 1 and len(sp) != 2:
		return user, password, ip, port
	if len(sp) == 2:
		usp = sp[0].split(':')
		if len(usp) != 2:
			return user, password, ip, port
		user = usp[0]
		password = usp[1]
	hsp = sp[len(sp)-1].split(':')
	if len(hsp) != 2:
		return user, password, ip, port
	ip = hsp[0]
	port = int(hsp[1])
	return user, password, ip, port

def GetFieldList(fields):
	fieldList = []
	for field in fields.split(','):
		if field.strip() != '':
			fieldList.append(field.strip())
	return fieldList

def initArgs():
	parser = OptionParser()
	parser.add_option('-a', '--host1', type='string', default='', dest='host1', help='mongo host [user:password@ip:port]')
	parser.add_option('-b', '--host2', type='string', default='', dest='host2', help='mongo host [user:password@ip:port]')
	parser.add_option('-d', '--db', type='string', default='', dest='db', help='database')
	parser.add_option('-c', '--col', type='string', default='', dest='col', help='collection')
	parser.add_option('-f', '--fields', type='string', default='', dest='fields', help='compare fields ["+field1,field2,field3" or "-field1,field2,field3"]')
	parser.add_option('-s', '--show', type='string', default='', dest='show', help='show fields ["field1,field2,field3"]')
	return parser

def run():
	parser = initArgs()
	opts, _ = parser.parse_args()
	host1 = opts.host1
	host2 = opts.host2
	dbName = opts.db
	colName = opts.col
	fields = opts.fields
	show = opts.show
	if fields == '':
		cmpMode = CMP_EXCLUDE
		fieldList = []
	elif fields[0] == '+':
		cmpMode = CMP_INCLUDE
		fieldList = GetFieldList(fields[1:])
	elif fields[0] == '-':
		cmpMode = CMP_EXCLUDE
		fieldList = GetFieldList(fields[1:])
	else:
		cmpMode = CMP_INCLUDE
		fieldList = GetFieldList(fields)
	showList = GetFieldList(show)

	user1, pwd1, ip1, port1 = GetHostInfo(host1)
	user2, pwd2, ip2, port2 = GetHostInfo(host2)

	client1 = Client(ip1, port1, user1, pwd1)
	client2 = Client(ip2, port2, user2, pwd2)
	dataSource1 = DataSource(client1, '%s:%s' % (ip1, port1))
	dataSource2 = DataSource(client2, '%s:%s' % (ip2, port2))
	comparer = Comparer(dataSource1, dataSource2, cmpMode, fieldList, showList)
	comparer.run(dbName, colName)

if __name__ == '__main__':
	run()
