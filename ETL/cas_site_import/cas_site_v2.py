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
        f = open('/home/kulen/Documents/official_cas.csv', 'rb')
        reader = csv.reader(f)
        for row in reader:
            logging.info(u"处理URL:%s", row[5])
            url_tmp = row[5].strip()
            if not url_tmp:
                continue
            url_tmp = url_tmp.lower()
            if not url_tmp.startswith('http://'):
                url_tmp = 'http://' + url_tmp
            if not url_tmp.startswith('http://'):
                continue
            url = urlparse.urlparse(url_tmp)
            if not url.hostname:
                continue
            if url.hostname == 'http':
                continue
            if self.get_check_site(url.hostname):
                continue
            
            logging.info(u"将数据:%s 写入数据库", url.hostname)
            sql = '''
                insert into spider_site (name,domain,all_spider,use_proxy,seed_url) 
                values (%s, %s, 1, -1, %s) 
            '''
            self.db_spider.insert(sql, row[3], url.hostname, url_tmp)
            
            site_id = self.get_check_site(url.hostname)
            sql = '''insert into processor_site (site_id,domain,processor,seed_url,status,refer1,refer2)
                values (%s,%s,'CasExtractSpiderV3',%s,0,%s,%s)
            '''
            self.db_spider.insert(sql, site_id, url.hostname, url_tmp, row[1], row[7])
        logging.info("完成数据读取")
    
    def get_check_site(self, domain):
        sql = 'select * from spider_site where domain=%s'
        check_rs = self.db_spider.query(sql, domain)
        for r in check_rs:
            return r['id']
        return None
        
    def sql(self):
        "insert into spider.spider_site (id,name,domain,processor,all_spider,speed,once_num,seed_url) select id,name,domain,'CasExtractSpiderV3',1,3000,8,url from spider_data.cas_extract_site where id>13649 on duplicate key update last_update_time=now()"
        # id>12639

if __name__ == '__main__':

    reload(sys)
    sys.setdefaultencoding('utf-8')
    
    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="/tmp/cas_site_v2.log", help="NSQ topic")
    options.parse_command_line()
    logfile = options.logfile
    fmt = '%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s'
    handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=100 * 1024 * 1024, backupCount=10)  # 实例化handler
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt)  # 实例化formatter
    handler.setFormatter(formatter)  # 为handler添加formatter
    logging.getLogger('').addHandler(handler)
    logging.info(u'写入的日志文件为:%s', logfile)
    
    cs = CasSite()
    cs.write_cas_data()
    # print urllib.urlencode({'url':"http://www.bd.com/a?ab=user"})
    
    
