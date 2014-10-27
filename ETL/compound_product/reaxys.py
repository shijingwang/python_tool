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

class ReaxyExtract(object):
    
    def __init__(self):
        self.db_spider_data = ConUtil.connect_mysql(settings.MYSQL_SPIDER_DATA)
        self.db_dict_source = ConUtil.connect_mysql(settings.MYSQL_DICT_SOURCE)
        pass
    
    def extract_moldata(self):
        sql = 'select * from compound_product where site_id=2'
        logging.info(u"sql语句为:%s", sql)
        cps = self.db_spider_data.query(sql)
        logging.info(u"查询出来的记录数为:%s", len(cps))
        if len(cps) == 0:
            return
        for cp in cps:
            # java value
            logging.info(u"处理id:%s, CAS号:%s", cp['id'], cp['cas'])
            jv = json.loads(cp['_value'])
            if 'mol' not in jv or len(jv['mol'].strip()) < 10:
                logging.warn("id:%s cas:%s 无mol数据", cp['id'], cp['cas'])
                continue
            sql = 'insert into dic_source_data (has_dispose,name_en,write_type,data_type,cas_no,mol) values (1, %s, 1, 2, %s, %s)'
            self.db_dict_source.execute(sql, cp['name'], cp['cas'], jv['mol'])
    
    def update_molid(self):
        sql = "select refer1,query from spider_query_data where name='dict'"
        rs = self.db_spider_data.query(sql)
        
        sql = "select cas_no from dic_source_data"
        trs = self.db_dict_source.query(sql)
        
        for tr in trs:
            logging.info(u'处理CAS号:%s', tr['cas_no'])
            for r in rs:
                if r['query'] == tr['cas_no']:
                    sql = 'update dic_source_data set mol_id=%s,write_type=2 where cas_no=%s'
                    logging.info(u'更新cas:%s mol_id:%s', tr['cas_no'], r['refer1'])
                    self.db_dict_source.execute(sql, r['refer1'], tr['cas_no'])
                    break

if __name__ == '__main__':

    reload(sys)
    sys.setdefaultencoding('utf-8')
    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="/tmp/etl.log", help="NSQ topic")
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
    re = ReaxyExtract()
    re.update_molid()
    logging.info(u'程序运行完成')
