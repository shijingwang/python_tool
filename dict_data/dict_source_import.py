# -*- coding: utf-8 -*-
from tornado.options import define, options
import logging
import os, sys
import csv
import signal
import traceback
import time
try:
    import python_tool
except ImportError:
    fp = os.path.abspath(__file__)
    sys.path.append(fp[0:fp.rfind('python_tool') + 11])
from common.con_util import ConUtil
from common.cas_util import CasUtil
import dict_conf

class DictSourceImport(object):
    
    def __init__(self):
        self.db_dict_source = ConUtil.connect_mysql(dict_conf.MYSQL_DICT_SOURCE)
        pass
    
    def readCsv(self):
        f = open('/home/kulen/Documents/zinc汇总数据.csv', 'rb')
        reader = csv.reader(f)
        counter = 0
        for row in reader:
            counter += 1
            if counter == 1:
                continue
            cas = ''
            if not row[1] or len(row[1].strip()) > 0:
                cas = row[1].replace('`', '')
            logging.info("%s %s %s %s", row[0], cas, row[2], row[3])
            sql = 'insert into dic_source_data (name_en,data_type,cas_no,smiles) values (%s,1,%s,%s)'
            # self.db_dict_source.insert(sql, [row[3], cas, row[2]])
            self.db_dict_source.insert(sql, row[3], cas, row[2])
            if counter >= 10:
                break
        logging.info("完成数据导入")

if __name__ == '__main__':

    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="/home/kulen/log/run.log", help="NSQ topic")
    define("func_name", default="spider_apple")
    options.parse_command_line()
    logfile = options.logfile
    func_name = options.func_name
    logging.info(u'写入的日志文件为:%s', logfile)
    
    dsi = DictSourceImport()
    dsi.readCsv()
