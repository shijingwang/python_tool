# -*- coding: utf-8 -*-
from tornado.options import define, options
import logging
import os, sys
import traceback
import json
import csv
try:
    import python_tool
except ImportError:
    fp = os.path.abspath(__file__)
    sys.path.append(fp[0:fp.rfind('python_tool') + 11])
from common.con_util import ConUtil
import settings

class ExtractData(object):
    
    def __init__(self):
        self.db_spider_data = ConUtil.connect_mysql(settings.MYSQL_SPIDER_DATA)
        pass
    
    def extract_data(self):
        sql = 'select * from compound_product where site_id=12609'
        sql = sql
        logging.info(u"sql语句为:%s", sql)
        cps = self.db_spider_data.query(sql)
        logging.info(u"查询出来的记录数为:%s", len(cps))
        if len(cps) == 0:
            return
        csvfile = file('/tmp/hxchem.csv', 'wb')
        writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow(['CAS号', '中文名称', '中文别名', '英文名称', '英文别名', 'URL'])
        for cp in cps:
            # java value
            logging.info(u"处理CAS号:%s", cp['cas'])
            jv = json.loads(cp['_value'])
            row = ['`%s`' % cp['cas'], cp['name'], jv[u'中文别名'], jv[u'英文名称'], jv[u'英文别名'], jv[u'url']]
            writer.writerow(row)
        csvfile.close()
    
    def extract_chemical_data(self):
        sql = 'select * from compound_product where site_id=12607'
        sql = sql
        logging.info(u"sql语句为:%s", sql)
        cps = self.db_spider_data.query(sql)
        logging.info(u"查询出来的记录数为:%s", len(cps))
        if len(cps) == 0:
            return
        csvfile = file('/tmp/chemicalbook.csv', 'wb')
        writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow(['CAS号', '中文名', '英文名', 'price', 'package', 'release_date', 'supplier'])
        for cp in cps:
            # java value
            logging.info(u"处理CAS号:%s", cp['cas'])
            jv = json.loads(cp['_value'])
            row = ['`%s`' % cp['cas'], jv['name_cn'], jv['name_en'], jv['price'], jv['package'], jv['release_date'], jv['supplier']]
            writer.writerow(row)
        csvfile.close()

if __name__ == '__main__':

    reload(sys)
    sys.setdefaultencoding('utf-8')
    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="/tmp/cp_hxchem.log", help="NSQ topic")
    define("func_name", default="spider_apple")
    options.parse_command_line()
    logfile = options.logfile
    fmt = '%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s'
    handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=100 * 1024 * 1024, backupCount=10)  # 实例化handler
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt)  # 实例化formatter
    handler.setFormatter(formatter)  # 为handler添加formatter
    logging.getLogger('').addHandler(handler)
    logging.info(u'写入的日志文件为:%s', logfile)
    ed = ExtractData()
    ed.extract_chemical_data()
    logging.info(u'程序运行完成')
