# -*- coding: utf-8 -*-
'''
	@author: 	jemuelmiao
	@filename: 	const.py
	@create: 	2018-11-03 22:24:01
	@modify:	2018-11-05 13:32:53
	@desc:		
'''
# config文件
CONF_FILE = 'mongo2sql.config'
# db模式，包含、排除
DB_INCLUDE = 1
DB_EXCLUDE = 2
# field模式，包含、排除
FIELD_INVALID = 1
FIELD_INCLUDE = 2
FIELD_EXCLUDE = 3
# 处理模式，导出并导入、导出、导入
MODE_EXPORT = 1
MODE_IMPORT = 2
# 数据库类型
TYPE_MYSQL = 0
TYPE_PGSQL = 1
TYPE_INVALID = 2
# sql类型转换，列表对应数据库类型下标
TYPE_VARCHAR_10 = ['varchar(10)', 'varchar(10)']
TYPE_VARCHAR_50 = ['varchar(50)', 'varchar(50)']
TYPE_VARCHAR_100 = ['varchar(100)', 'varchar(100)']
TYPE_VARCHAR_300 = ['varchar(300)', 'varchar(300)']
TYPE_TEXT = ['text', 'text']
TYPE_MEDIUMTEXT = ['mediumtext', 'text']
TYPE_TINYINT = ['tinyint(2)', 'smallint']
TYPE_INT = ['int', 'int']
TYPE_BIGINT = ['bigint', 'bigint']
TYPE_DOUBLE = ['double', 'real']
