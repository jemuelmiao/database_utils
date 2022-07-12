# -*- coding: utf-8 -*-
'''
	@author: 	jemuelmiao
	@filename: 	copy_database.py
	@create: 	2018-05-25 11:28:17
	@modify:	2021-09-27 13:09:09
	@desc:		拷贝数据库，包括索引
'''
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from subprocess import Popen, PIPE
from optparse import OptionParser
from bson.son import SON
import os
import re
import sys
if sys.version.startswith('3'):
	import urllib.parse as urllib_
else:
	import urllib as urllib_

# 处理模式，导出并导入、导出、导入
MODE_ALL	= 0
MODE_EXPORT = 1
MODE_IMPORT = 2
# 临时文件目录
TEMPORARY_DIR = '__temporary__'
# 导入导出指令
EXPORTER_CMD = 'mongoexport'
IMPORTER_CMD = 'mongoimport'
# db模式，包含、排除
DB_INCLUDE = 1
DB_EXCLUDE = 2
# 正式环境不能import
FormatIpList = ["127.0.0.1"]

class ColInfo(object):
	INDEX_OPTIONS = ['name', 'unique', 'background', 'sparse', 'bucketSize', 'min', 'max', \
					'expireAfterSeconds', 'partialFilterExpression', 'collation']
	def __init__(self, col):
		super(ColInfo, self).__init__()
		self._col = col

	@property
	def name(self):
		return self._col.name

	@property
	def count(self):
		return self._col.count()

	def getIndex(self):
		indexList = []
		indexInfo = self._col.index_information()
		for name, info in indexInfo.iteritems():
			if name == '_id_':
				continue
			# if k[1] == '2dsphere':
			keys = [(k[0], int(k[1])) if k[1] != '2dsphere' else (k[0], k[1]) for k in info.get('key', [])]
			opts = {}
			for opt in self.INDEX_OPTIONS:
				if opt in info:
					opts[opt] = info[opt]
			indexList.append((keys, opts))
		return indexList

	def setIndex(self, indexList):
		for info in indexList:
			self._col.create_index(info[0], **info[1])

class DBInfo(object):
	def __init__(self, db):
		super(DBInfo, self).__init__()
		self._db = db
		self._colList = []

	def addCol(self, col):
		self._colList.append(col)

	def dropCol(self, colName):
		for colInfo in self._colList:
			if colInfo.name == colName:
				self._colList.remove(colInfo)
				self._db.drop_collection(colName)
				return

	@property
	def name(self):
		return self._db.name

	@property
	def colList(self):
		return self._colList

class IExporter(object):
	def __init__(self, client):
		super(IExporter, self).__init__()
		self._client = client

	# 获取目录下所有数据库名
	def getDirs(self, path):
		dirs = []
		for filename in os.listdir(path):
			if os.path.isdir(os.path.join(path, filename)):
				dirs.append(filename)
		return dirs

	# 获取目录下所有表名
	def getFiles(self, path):
		files = []
		for filename in os.listdir(path):
			if os.path.isfile(os.path.join(path, filename)) and filename.startswith('d_'):
				files.append(filename[2:])
		return files

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

class Exporter(IExporter):
	def __init__(self, client):
		# self._client = client
		super(Exporter, self).__init__(client)

	def run(self, dir, dbMode, dbNames, force=True):
		print('export run===========================')
		if not os.path.isdir(dir):
			os.mkdir(dir)
		host = self._client.host
		port = self._client.port
		user = self._client.user
		password = self._client.password
		dbFileDict = {}
		if not force:
			allDbs = self.getDirs(dir)
			for dbName in allDbs:
				colList = self.getFiles(os.path.join(dir, dbName))
				dbFileDict[dbName] = colList
		dbList = self._client.GetDbList()
		for dbInfo in dbList:
			dbName = dbInfo.name
			if dbMode == DB_EXCLUDE:
				if dbName in dbNames:
					continue
			else:
				if dbName not in dbNames:
					continue
			path = os.path.join(dir, dbName)
			if not os.path.isdir(path):
				os.mkdir(path)
			for colInfo in dbInfo.colList:
				colName = colInfo.name
				if not force and dbName in dbFileDict and colName in dbFileDict[dbName]:
					print('using exist %s.%s'%(dbName, colName))
					continue
				dataFile = os.path.join(path, 'd_' + colName)
				indexFile = os.path.join(path, 'i_' + colName)
				exportcmd = '"%s" -h %s --authenticationDatabase admin --port %d -d %s -c %s -o %s' % (
						EXPORTER_CMD, host, port, dbName, colName, dataFile)
				if user:
					exportcmd += ' -u %s' %(user)
				if password:
					exportcmd += ' -p %s' %(urllib_.unquote(password))
				# print(exportcmd)
				print('exporting %s.%s'%(dbName, colName))
				p = Popen(exportcmd, shell=True, stdout=PIPE, stderr=PIPE)
				p.wait()
				errContent = p.stderr.read()
				matchObj = re.search(r'exported (\d+) record', errContent)
				if matchObj:
					real = int(matchObj.group(1))
					need = colInfo.count
					if real < need:
						print('\twarn:%d/%d' % (need, real))
					else:
						print('\tsucc:%d/%d' % (need, real))
					indexList = colInfo.getIndex()
					with open(indexFile, 'w') as fp:
						fp.write(str(indexList))
				else:
					print('\tfail:%s' % (errContent))
					if os.path.isfile(dataFile):
						os.remove(dataFile)

class Importer(IExporter):
	def __init__(self, client):
		super(Importer, self).__init__(client)

	# 记录下没有文件的和表名冲突的信息
	def run(self, dir, dbMode, dbNames, force=False):
		print('import run===========================')
		if not os.path.isdir(dir):
			return
		allDbs = self.getDirs(dir)
		validDbDict = {}
		invalidDbs = []
		for dbName in allDbs:
			colList = self.getFiles(os.path.join(dir, dbName))
			if len(colList) == 0:
				invalidDbs.append(dbName)
				continue
			if dbMode == DB_EXCLUDE:
				if dbName not in dbNames:
					validDbDict[dbName] = colList
				else:
					invalidDbs.append(dbName)
			else:
				if dbName in dbNames:
					validDbDict[dbName] = colList
				else:
					invalidDbs.append(dbName)
		host = self._client.host
		port = self._client.port
		user = self._client.user
		password = self._client.password
		conflictDict = {}
		dbList = self._client.GetDbList()
		for dbInfo in dbList:
			dbName = dbInfo.name
			if dbName not in validDbDict:
				continue
			for colInfo in dbInfo.colList:
				colName = colInfo.name
				if colName not in validDbDict[dbName]:
					continue
				if dbName not in conflictDict:
					conflictDict[dbName] = []
				conflictDict[dbName].append(colName)
				# dropDict[dbName]
				if not force:
					validDbDict[dbName].remove(colName)
					if len(validDbDict[dbName]) == 0:
						validDbDict.pop(dbName)
		# 根据validDbDict信息import
		print('invalid database:', self.pt(invalidDbs))
		print('conflict collection:', self.pt(conflictDict))
		for dbName, colNames in validDbDict.iteritems():
			path = os.path.join(dir, dbName)
			for colName in colNames:
				# system.开头的表导入会出错
				if colName.startswith('system.'):
					continue
				dataFile = os.path.join(path, 'd_' + colName)
				if not os.path.isfile(dataFile):
					continue
				if dbName in conflictDict and colName in conflictDict[dbName]:
					for dbInfo in dbList:
						if dbInfo.name == dbName:
							dbInfo.dropCol(colName)
							break
				importcmd = '"%s" -h %s  --authenticationDatabase admin --port %d -d %s -c %s --file %s' % (
							IMPORTER_CMD, host, port, dbName, colName, dataFile)
				if user:
					importcmd += ' -u %s' % (user)
				if password:
					importcmd += ' -p %s' % (urllib_.unquote(password))
				print('importing %s.%s'%(dbName, colName))
				p = Popen(importcmd, shell=True, stdout=PIPE, stderr=PIPE)
				p.wait()
				errContent = p.stderr.read()
				matchObj = re.search(r'imported (\d+) document', errContent)
				if matchObj:
					real = int(matchObj.group(1))
					print('\tsucc:%d' % (real))
				else:
					print('\tfail:%s' % (errContent))
		dbList = self._client.GetDbList(validDbDict.keys())
		for dbInfo in dbList:
			dbName = dbInfo.name
			path = os.path.join(dir, dbName)
			for colInfo in dbInfo.colList:
				colName = colInfo.name
				indexFile = os.path.join(path, 'i_' + colName)
				if not os.path.isfile(indexFile):
					continue
				with open(indexFile, 'r') as fp:
					indexList = eval(fp.read())
					print('create index %s.%s'%(dbName, colName))
					colInfo.setIndex(indexList)

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

	# 目的库需要dbNames为None，读取所有db，从文件中导入db中没有的col
	# 源库dbNames为需要导出库的列表，可能为None，就是导出所有库
	def GetDbList(self, dbNames=None):
		dbList = []
		for dbName in self._client.database_names():
			if dbNames is not None and dbName not in dbNames:
				continue
			db = Database(self._client, dbName)
			dbInfo = DBInfo(db)
			for colName in db.collection_names():
				col = db.get_collection(colName)
				colInfo = ColInfo(col)
				dbInfo.addCol(colInfo)
			dbList.append(dbInfo)
		return dbList

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

def GetDbNames(dbs):
	dbNames = []
	for name in dbs.split(','):
		if name.strip() != '':
			dbNames.append(name.strip())
	return dbNames

def GetTempDir():
	return TEMPORARY_DIR

def initArgs():
	parser = OptionParser()
	parser.add_option('-s', '--src', type='string', default='', dest='source', help='source [user:password@ip:port]')
	parser.add_option('-d', '--dst', type='string', default='', dest='dest', help='dest [user:password@ip:port]')
	parser.add_option('-m', '--mode', type='int', default=MODE_ALL, dest='mode', help='mode [1:export, 2:import, default:export and import]')
	parser.add_option('-b', '--dbs', type='string', default='', dest='dbs', help='dbs ["+db1,db2,db3" or "-db1,db2,db3"]')
	# 强制导入
	parser.add_option('-f', '--force', action='store_true', default=False, dest='force', help='import force, drop dest conflict collection')
	# 使用已有文件
	parser.add_option('-e', '--exist', action='store_true', default=False, dest='exist', help='export use exist files')
	return parser

def run():
	parser = initArgs()
	opts, _ = parser.parse_args()
	source = opts.source
	dest = opts.dest
	mode = opts.mode
	dbs = opts.dbs
	force = opts.force
	exist = opts.exist
	srcUser, srcPwd, srcIp, srcPort = GetHostInfo(source)
	dstUser, dstPwd, dstIp, dstPort = GetHostInfo(dest)
	if len(dbs) == 0:
		print('db empty!!!')
		parser.print_help()
		exit(0)
	if dbs[0] == '+':
		dbs = dbs[1:]
		dbMode = DB_INCLUDE
	elif dbs[0] == '-':
		dbs = dbs[1:]
		dbMode = DB_EXCLUDE
	else:
		dbMode = DB_INCLUDE
	dbNames = GetDbNames(dbs)
	if len(dbNames) == 0:
		print('db empty!!!')
		parser.print_help()
		exit(0)
	tempDir = GetTempDir()
	if not os.path.isdir(tempDir):
		os.mkdir(tempDir)
	if mode == MODE_ALL:
		if srcIp == '' or dstIp == '' or dstIp in FormatIpList or (srcIp == dstIp and srcPort == dstPort):
			print('ip or port invalid!!!')
			parser.print_help()
			exit(0)
		srcClient = Client(srcIp, srcPort, srcUser, srcPwd)
		dstClient = Client(dstIp, dstPort, dstUser, dstPwd)
		exporter = Exporter(srcClient)
		exporter.run(tempDir, dbMode, dbNames, not exist)
		importer = Importer(dstClient)
		importer.run(tempDir, dbMode, dbNames, force)
	elif mode == MODE_EXPORT:
		if srcIp == '':
			print('source ip invalid!!!')
			parser.print_help()
			exit(0)
		srcClient = Client(srcIp, srcPort, srcUser, srcPwd)
		exporter = Exporter(srcClient)
		exporter.run(tempDir, dbMode, dbNames, not exist)
	elif mode == MODE_IMPORT:
		if dstIp == '' or dstIp in FormatIpList:
			print('dest ip invalid!!!')
			parser.print_help()
			exit(0)
		dstClient = Client(dstIp, dstPort, dstUser, dstPwd)
		importer = Importer(dstClient)
		importer.run(tempDir, dbMode, dbNames, force)
	else:
		print('unknown mode!!!')
		parser.print_help()
		exit(0)

if __name__ == '__main__':
	run()
