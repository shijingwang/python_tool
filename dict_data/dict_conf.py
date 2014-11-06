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
chemdraw_work_dir = "C:\\ChemDraw"
chemdraw_app = "C:\\Program Files (x86)\\CambridgeSoft\\ChemOffice2010\\ChemDraw\\ChemDraw.exe"
nmr_img_clean = "C:\\ChemDraw\\clean_app\\img-clean.exe"  # 必须为双反斜杠，否则指令不会执行
MARK_LOGO_IMG = 'C:/ChemDraw/mark_logov3/logov3_60.png'
SDF_RESULT_PATH = "/tmp/"
scalingfactor = 0.22
mol2psopt = "--rotate=auto3Donly --hydrogenonmethyl=off"
# REDIS_SERVER = {'host':'127.0.0.1', 'port':6380, 'password':'molbase1010'}
REDIS_SERVER = {'host':'192.168.0.124', 'port':6379, 'password':''}
# 解决公司环境无法直接连接到亚马逊Redis, 对数据进行相应的中转
TRANSFER_REDIS_SERVER = {'host':'192.168.0.223', 'port':6380, 'password':'molbase1010'}
