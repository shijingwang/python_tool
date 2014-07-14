# -*- coding: utf-8 -*-
from common.con_util import ConUtil
import settings
import logging
import time
import traceback
from tornado.options import define, options

class DataCheck(object):
    
    def __init__(self):
        self.db_molbase = ConUtil.connect_mysql(settings.MYSQL_MOLBASE)
    
    def view_data(self):
        struc_sql = 'select * from search_molstruc'
        sdata = self.db_molbase.query(struc_sql)
        for sd in sdata:
            print sd['mol_id']
            print sd['struc']

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')

    define("logfile", default="/tmp/py.log", help="NSQ topic")
    options.parse_command_line()
    logfile = options.logfile
    logging.info(u'写入的日志文件为:%s', logfile)
    # 自动对日志文件进行分割
    fmt = '%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s'
    handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=100 * 1024 * 1024, backupCount=10)  # 实例化handler
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt)  # 实例化formatter
    handler.setFormatter(formatter)  # 为handler添加formatter
    logging.getLogger('').addHandler(handler)
    nc = DataCheck()
    nc.view_data()