# -*- coding: utf-8 -*-
MYSQL_DICT_WORKER = {"host": "127.0.0.1",
               "port": "3306",
               "database": "molbase",
               "user": "root",
               "password": "rainzgq"
               }
MYSQL_DICT_AGENT = {"host": "127.0.0.1",
               "port": "3306",
               "database": "molbase_check",
               "user": "root",
               "password": "rainzgq"
               }
MYSQL_DICT_SOURCE = {"host": "127.0.0.1",
               "port": "3306",
               "database": "dic_cas",
               "user": "root",
               "password": "rainzgq"
               }
CHECKMOL = "/home/kulen/checkmol04/checkmol"
MATCHMOL = "/home/kulen/checkmol04/matchmol"
CHECKMOL_V2 = "/home/kulen/checkmol04/molstat"
MATCHMOL_V2 = "/home/kulen/checkmol04/molmatch"
MOL2PS = "/usr/local/bin/mol2ps"
GHOSTSCRIPT = "/usr/bin/gs"
agent_bitmapdir = "/home/kulen/molpic"
worker_bitmapdir = "/home/kulen/molpic2"
agent_nmr_picdir = "/home/kulen/nmrpic"
SDF_RESULT_PATH = "/tmp/"
scalingfactor = 0.22
mol2psopt = "--rotate=auto3Donly --hydrogenonmethyl=off"
REDIS_SERVER = {'host':'127.0.0.1', 'port':6380, 'password':'molbase1010'}
