# -*- coding: utf-8 -*-
MYSQL_DICT = {"host": "127.0.0.1",
               "port": "3306",
               "database": "molbase",
               "user": "root",
               "password": "rainzgq"
               }
MYSQL_DICT_SOURCE = {"host": "127.0.0.1",
               "port": "3306",
               "database": "dic_cas",
               "user": "root",
               "password": "rainzgq"
               }
APP_PATH = 'C:\\Users\\Administrator\\Desktop\\nmr_origin\\'
SAVE_PATH = 'F:/file/'
MOL_FILE_PATH = "D:/molfile/"
NMR_PIC_PATH = "D:/nmrpic/"
CHEM_ACX_MOLFILE_PATH = "D:/chemacx_molfile/"
MAIL_SEND_SERVER = "smtp.molbase.com"
MAIL_USER = "guoqiang.zhang@molbase.com"
MAIL_PASSWORD = "rainzgq"
try:
    from settings_local import *
except ImportError:
    pass
