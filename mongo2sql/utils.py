# -*- coding: utf-8 -*-
'''
	@author: 	jemuelmiao
	@filename: 	utils.py
	@create: 	2018-11-03 22:23:17
	@modify:	2018-11-05 13:34:13
	@desc:		
'''
import const

def getHostInfo(info):
	user = ''
	password = ''
	ip = ''
	port = -1
	db = ''
	sp = info.strip().split('@')
	if len(sp) != 1 and len(sp) != 2:
		return user, password, ip, port, db
	if len(sp) == 2:
		usp = sp[0].split(':')
		if len(usp) != 2:
			return user, password, ip, port, db
		user = usp[0]
		password = usp[1]
	hsp = sp[len(sp)-1].split(':')
	if len(hsp) != 2:
		return user, password, ip, port, db
	ip = hsp[0]
	pdb = hsp[1].split('/')
	port = int(pdb[0])
	if len(pdb) > 1:
		db = pdb[1]
	return user, password, ip, port, db

def getDbNames(dbs):
	dbNames = []
	for name in dbs.split(','):
		if name.strip() != '':
			dbNames.append(name.strip())
	return dbNames