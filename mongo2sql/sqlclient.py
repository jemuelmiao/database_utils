# -*- coding: utf-8 -*-
'''
	@author: 	jemuelmiao
	@filename: 	sql.py
	@create: 	2018-11-03 22:18:21
	@modify:	2018-11-05 20:23:02
	@desc:		
'''
import mysql.connector
import psycopg2
import const

class BaseSql(object):
	def execute(self, cmd):
		cursor = self.conn.cursor()
		cursor.execute(cmd)
		self.conn.commit()
		cursor.close()

class Mysql(BaseSql):
	def __init__(self, host, port, user, password, database):
		super(Mysql, self).__init__()
		self.conn = mysql.connector.connect(host=host, port=port, user=user, password=password, database=database, use_unicode=True)

class Pgsql(BaseSql):
	def __init__(self, host, port, user, password, database):
		super(Pgsql, self).__init__()
		self.conn = psycopg2.connect(host=host, port=port, user=user, password=password, database=database)
		# 禁用事务，因为前一个insert出错后，数据库处于错误状态，后续insert会一直出错
		self.conn.set_isolation_level(0)

def getSqlClient(sqlType, host, port, user, password, database):
	if sqlType == const.TYPE_MYSQL:
		return Mysql(host, port, user, password, database)
	elif sqlType == const.TYPE_PGSQL:
		return Pgsql(host, port, user, password, database)
	else:
		return None