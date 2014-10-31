# -*- coding: utf-8 -*-
import json
import logging
import os, sys
from tornado.options import define, options
import traceback
import time
import csv
try:
    import python_tool
except ImportError:
    fp = os.path.abspath(__file__)
    sys.path.append(fp[0:fp.rfind('python_tool') + 11])
import CK
from common.con_util import ConUtil
from common.cas_util import CasUtil
from dict_compound import DictCompound
import dict_conf


class DictDataFix(object):
    
    def __init__(self):
        self.db_dict = ConUtil.connect_mysql(dict_conf.MYSQL_DICT_WORKER)
        pass;
    
    def generate_sql(self):
        f = open('/home/kulen/Documents/dict_fix.csv', 'rb')
        reader = csv.reader(f)
        counter = 0
        f=open('/home/kulen/Documents/mol_name.sql','w')  
        for row in reader:
            counter += 1
            if counter == 1:
                continue
            sql = 'update search_moldata set mol_name=\'%s\' where mol_id=%s;' % (row[2].replace("'","''"), row[0]);
            f.writelines(sql + "\n")
            #print sql
            #self.db_dict.execute(sql)
        f.close    
        logging.info("完成数据导入")
        
if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')
    
    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="/tmp/dict_util.log", help="NSQ topic")
    define("func_name", default="import_table_data")
    options.parse_command_line()
    logfile = options.logfile
    fmt = '%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s'
    handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=100 * 1024 * 1024, backupCount=10)  # 实例化handler
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt)  # 实例化formatter
    handler.setFormatter(formatter)  # 为handler添加formatter
    logging.getLogger('').addHandler(handler)
    logging.info(u'写入的日志文件为:%s', logfile)
    
    ddf = DictDataFix()
    ddf.generate_sql()
    # du.import_table_data()
    # du.write_redis_data()
    # du.string_test()
    logging.info(u'完成初始化!');