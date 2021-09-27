# -*- coding: utf-8 -*-
'''
	@author: 	jemuelmiao
	@filename: 	config.py
	@create: 	2018-11-03 22:22:01
	@modify:	2018-11-05 13:32:11
	@desc:		
'''
import ConfigParser
import const
import sys

conf = ConfigParser.SafeConfigParser()
conf.read(const.CONF_FILE)

def get(section, key):
	if conf.has_option(section, key):
		return conf.get(section, key)
	return ''

def getQuote():
	sqlType = getSqlType()
	if sqlType == const.TYPE_MYSQL:
		return '`'
	elif sqlType == const.TYPE_PGSQL:
		return '"'
	else:
		return ''

def getFields(dbName, colName):
	data = get('%s_%s'%(dbName, colName), 'fields')
	if data == '':
		return const.FIELD_INVALID, None
	if len(data) == 0:
		return const.FIELD_INVALID, None
	mode = const.FIELD_INCLUDE
	if data[0] == '-':
		mode = const.FIELD_EXCLUDE
		data = data[1:]
	elif data[0] == '+':
		data = data[1:]
	return mode, [f.strip() for f in data.split(',')]
	
def getPrimary(dbName, colName):
	data = get('%s_%s'%(dbName, colName), 'primary')
	quote = getQuote()
	if data == '':
		fields = ['%s_id%s'%(quote, quote)]
	else:
		fields = ['%s%s%s'%(quote, f.strip(), quote) for f in data.split(',')]
		if len(fields) == 0:
			fields = ['%s_id%s'%(quote, quote)]
	return '\tprimary key (%s)'%(','.join(fields))

def getUniques(dbName, colName):
	data = get('%s_%s'%(dbName, colName), 'uniques')
	if data == '':
		return ''
	quote = getQuote()
	uniques = [['%s%s%s'%(quote, f.strip(), quote) for f in u.split(',')] for u in data.split('|')]
	uniques = ['\tunique (%s),'%(','.join(u)) for u in uniques]
	return '\n'.join(uniques)[:-1]

def getForeigns(dbName, colName):
	data = get('%s_%s'%(dbName, colName), 'foreigns')
	if data == '':
		return '', None
	quote = getQuote()
	refTables = []
	foreigns = []
	for d in data.split('|'):
		items = d.split(':')
		tableName = items[1].strip()
		srcFields = ['%s%s%s'%(quote, m.strip(), quote) for m in items[0].split(',')]
		dstFields = ['%s%s%s'%(quote, m.strip(), quote) for m in items[2].split(',')]
		srcStr = ','.join(srcFields)
		dstStr = ','.join(dstFields)
		if tableName not in refTables:
			refTables.append(tableName)
		foreigns.append('\tforeign key (%s) references %s%s%s (%s),'%(srcStr, quote, tableName, quote, dstStr))
	return '\n'.join(foreigns)[:-1], refTables

def getDataLimit(dbName, colName):
	data = get('%s_%s'%(dbName, colName), 'datalimit')
	if data == '':
		return sys.maxint
	return int(data)

def getIgnoreTables():
	section = '__extra__'
	key = 'ignoretables'
	data = get(section, key)
	if data == '':
		return None
	return [c.strip() for c in data.split(',')]

def getSqlType():
	section = '__extra__'
	key = 'sqltype'
	data = get(section, key)
	if data == '':
		return const.TYPE_INVALID
	sqlType = data.lower()
	if sqlType == 'mysql':
		return const.TYPE_MYSQL
	elif sqlType == 'pgsql':
		return const.TYPE_PGSQL
	else:
		return const.TYPE_INVALID

def getListDepth():
	section = '__extra__'
	key = 'listdepth'
	data = get(section, key)
	if data == '':
		return 4
	return int(data)

def getSqlDir():
	return get('__extra__', 'sqldir')

def getAddBatchFile():
	return get('__extra__', 'addbatchfile')

def getDelBatchFile():
	return get('__extra__', 'delbatchfile')
