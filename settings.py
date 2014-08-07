# -*- coding: utf-8 -*-
MYSQL_MOLBASE = {"host": "127.0.0.1",
               "port": "3306",
               "database": "molbase",
               "user": "root",
               "password": ""
               }
APP_PATH = 'C:\\Users\\Administrator\\Desktop\\nmr_origin\\'
SAVE_PATH = 'F:/file/'
MOL_FILE_PATH = "D:/molfile/"
NMR_PIC_PATH = "D:/nmrpic/"
try:
    from settings_local import *
except ImportError:
    pass
