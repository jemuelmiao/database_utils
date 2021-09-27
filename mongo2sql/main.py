# -*- coding: utf-8 -*-
'''
	@author: 	jemuelmiao
	@filename: 	main.py
	@create: 	2018-11-03 22:25:12
	@modify:	2018-11-05 14:48:52
	@desc:		
'''
from optparse import OptionParser
from mongo import Mongo
from converter import Exporter, Importer
import sqlclient
import const
import utils
import config
import os

def initArgs():
	parser = OptionParser()
	parser.add_option('-s', '--src', type='string', default='', dest='source', help='mongodb [user:password@ip:port]')
	parser.add_option('-d', '--dst', type='string', default='', dest='dest', help='sqldb [user:password@ip:port/db]')
	parser.add_option('-m', '--mode', type='int', default=const.MODE_EXPORT, dest='mode', help='mode [1:export, 2:import, default:export sql]')
	parser.add_option('-b', '--dbs', type='string', default='', dest='dbs', help='dbs ["+db1,db2,db3" or "-db1,db2,db3"]')
	return parser

def run():
	sqlDir = config.getSqlDir()
	if not os.path.isdir(sqlDir):
		os.mkdir(sqlDir)
	parser = initArgs()
	opts, _ = parser.parse_args()
	source = opts.source
	dest = opts.dest
	mode = opts.mode
	dbs = opts.dbs
	srcUser, srcPwd, srcIp, srcPort, _ = utils.getHostInfo(source)
	dstUser, dstPwd, dstIp, dstPort, dstDb = utils.getHostInfo(dest)
	if len(dbs) == 0:
		print 'db empty!!!'
		parser.print_help()
		exit(0)
	if dbs[0] == '+':
		dbs = dbs[1:]
		dbMode = const.DB_INCLUDE
	elif dbs[0] == '-':
		dbs = dbs[1:]
		dbMode = const.DB_EXCLUDE
	else:
		dbMode = const.DB_INCLUDE
	dbNames = utils.getDbNames(dbs)
	if mode == const.MODE_EXPORT:
		if srcIp == '':
			print 'source ip invalid!!!'
			parser.print_help()
			exit(0)
		mongo = Mongo(srcIp, srcPort, srcUser, srcPwd)
		exporter = Exporter(mongo, dbMode, dbNames)
		exporter.run()
	elif mode == const.MODE_IMPORT:
		if srcIp == '' or dstIp == '' or (srcIp == dstIp and srcPort == dstPort):
			print 'ip or port invalid!!!'
			parser.print_help()
			exit(0)
		mongo = Mongo(srcIp, srcPort, srcUser, srcPwd)
		sqlClient = sqlclient.getSqlClient(config.getSqlType(), dstIp, dstPort, dstUser, dstPwd, dstDb)
		importer = Importer(mongo, dbMode, dbNames)
		importer.run(sqlClient)
	else:
		print 'unknown mode!!!'
		parser.print_help()
		exit(0)

if __name__ == '__main__':
	run()