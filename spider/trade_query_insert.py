# -*- coding: utf-8 -*-
from tornado.options import define, options
import logging
import os, sys
import csv
import traceback
try:
    import python_tool
except ImportError:
    fp = os.path.abspath(__file__)
    sys.path.append(fp[0:fp.rfind('python_tool') + 11])
from common.con_util import ConUtil
from common.cas_util import CasUtil
import settings

class TradeQueryInsert(object):
    
    def __init__(self):
        self.db_spider_data = ConUtil.connect_mysql(settings.MYSQL_SPIDER_DATA)
        pass
    
    # Zinc数据导入
    def readQueryData(self):
        f = open('/home/kulen/Documents/中文名称.csv', 'rb')
        reader = csv.reader(f)
        counter = 0
        sql = "insert into spider_query_data (site_id, refer1, refer2, query) values (%s,%s,%s,%s)"
        for row in reader:
            counter += 1
            if counter == 1:
                continue
            logging.info(u'处理mol_id:%s', row[0])
            r_counter = 0
            query_list = []
            
            try:
                for r in row:
                    r_counter += 1
                    if r_counter <= 3:
                        continue
                    if r:
                        query_list.append(r)
                if len(query_list) == 0:
                    continue
                params_list = []
                for query in query_list:
                    params_list.append((1, str(row[0]), str(row[1]), query))
                self.db_spider_data.insertmany(sql, params_list)
            except Exception, e:
                logging.info(u"处理mol_id:%s 时出错", row[0])
        logging.info(u"完成数据导入")

if __name__ == '__main__':

    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="/tmp/query_import.log", help="NSQ topic")
    define("func_name", default="spider_apple")
    options.parse_command_line()
    logfile = options.logfile
    func_name = options.func_name
    logging.info(u'写入的日志文件为:%s', logfile)
    
    tqi = TradeQueryInsert()
    # dsi.readCsv()
    tqi.readQueryData()
    # dsi.readFixCsv()
    logging.info(u'程序执行完成!')
