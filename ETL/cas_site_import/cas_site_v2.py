# -*- coding: utf-8 -*-

import csv
import logging
import os
import sys
from tornado import escape
from tornado.options import define, options
import urlparse

from common.con_util import ConUtil
import settings

class CasSite(object):
    
    def __init__(self):
        self.db_spider = ConUtil.connect_mysql(settings.MYSQL_SPIDER)
        self.db_spider_data = ConUtil.connect_mysql(settings.MYSQL_SPIDER_DATA)
    
    def write_cas_data(self):
        logging.info("开始读取")
        f = open('/home/kulen/Documents/网址汇总.csv', 'rb')
        reader = csv.reader(f)
        for row in reader:
            url = urlparse.urlparse(row[2].strip())
            if not url.hostname:
                continue
            if url.hostname == 'http':
                continue
            sql = 'select * from spider_site where domain=%s'
            check_rs = self.db_spider.query(sql, url.hostname)
            if len(check_rs) > 0:
                continue
            sql = 'select * from cas_extract_site where domain=%s'
            check_rs = self.db_spider_data.query(sql, url.hostname)
            if len(check_rs) > 0:
                continue
            logging.info(u'处理domain数据:%s', url.hostname)
            sql = '''insert into cas_extract_site (v_id,name,domain,url,status,create_time,last_update_time)
                values (%s,%s,%s,%s,%s,now(),now())
            '''
            self.db_spider_data.insert(sql, row[0], row[1], url.hostname, row[2], 0)
        logging.info("完成数据读取")
    
    def sql(self):
        "insert into spider.spider_site (id,name,domain,processor,all_spider,speed,once_num,seed_url) select id,name,domain,'CasExtractSpiderV3',1,3000,8,url from spider_data.cas_extract_site where id>12645 on duplicate key update last_update_time=now()"
        # id>12639

if __name__ == '__main__':

    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="/home/kulen/log/run.log", help="NSQ topic")
    define("func_name", default="spider_apple")
    options.parse_command_line()
    logfile = options.logfile
    func_name = options.func_name
    logging.info(u'写入的日志文件为:%s', logfile)
    
    cs = CasSite()
    cs.write_cas_data()
    # print urllib.urlencode({'url':"http://www.bd.com/a?ab=user"})
    
    
