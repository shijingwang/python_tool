# -*- coding: utf-8 -*-
import csv
import logging
import sys, os
from tornado.options import define, options
try:
    import python_tool
except ImportError:
    fp = os.path.abspath(__file__)
    sys.path.append(fp[0:fp.rfind('python_tool') + 11])
from common.con_util import ConUtil
import settings

class Report(object):
    
    def __init__(self):
        self.db_spider_data = ConUtil.connect_mysql(settings.MYSQL_SPIDER_DATA)
        self.db_spider = ConUtil.connect_mysql(settings.MYSQL_SPIDER)
        
    def statistic_db(self, start_time, stop_time):
        sql = "select distinct(site_id) as site_id from compound_product"
        site_ids = self.db_spider_data.query(sql)
        result_list = []
        # 价格下载数据统计
        for site_id in site_ids:
            logging.info(u"统计站点:%s 价格数据抓取情况", site_id['site_id'])
            result = {'site_id':site_id['site_id'], 'type':1}
            sql = "select count(*) as num from compound_product where site_id='%s'"
            sql = sql % site_id['site_id']
            rs = self.db_spider_data.query(sql)
            for r in rs:
                result['total_num'] = r['num']
            sql = "select count(*) as num from compound_product where create_time>='%s' and create_time<'%s' and site_id='%s'"
            sql = sql % (start_time, stop_time, site_id['site_id'])
            rs = self.db_spider_data.query(sql)
            for r in rs:
                result['today_num'] = r['num']
            result_list.append(result)
        
        sql = "select distinct(site_id) as site_id from file_download"
        site_ids = self.db_spider_data.query(sql)
        for site_id in site_ids:
            logging.info(u"统计站点:%s MSDS抓取情况", site_id['site_id'])
            result = {'site_id':site_id['site_id'], 'type':2}
            sql = "select count(*) as num from file_download where site_id='%s' and status='1'"
            sql = sql % site_id['site_id']
            rs = self.db_spider_data.query(sql)
            for r in rs:
                result['total_num'] = r['num']
            sql = "select count(*) as num from file_download where create_time>='%s' and create_time<'%s' and site_id='%s' and status='1'"
            sql = sql % (start_time, stop_time, site_id['site_id'])
            rs = self.db_spider_data.query(sql)
            for r in rs:
                result['today_num'] = r['num']
            result_list.append(result)
        
        csvfile = file('/tmp/report.csv', 'wb')
        writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow([u'类型', u'域名', u'总数量', u'新增数量'])
        for result in result_list:
            sql = "select * from spider_site where id='%s'"
            sql = sql % result['site_id']
            rs = self.db_spider.query(sql)
            for r in rs:
                result['domain'] = r['domain']
            dtype = u'价格' if result['type'] == 1 else u'MSDS' 
            writer.writerow([dtype, result['domain'], result['total_num'], result['today_num']])
        csvfile.close()
        pass
    
    def statistic_nmr(self, day):
        
        pass
    
    def list_file_dir(self, level, path):  
        for i in os.listdir(path):
            os.path.isdir(path + '/' + i)  
            if os.path.isdir(path + '/' + i):  
                self.list_file_dir(level + 1, path + '/' + i)
            else:
                self.file_list.append(path + '/' + i)
    
    def send_mail(self):
        
        pass

if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')
    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="/tmp/run.log", help="NSQ topic")
    define("func_name", default="spider_apple")
    options.parse_command_line()
    logfile = options.logfile
    func_name = options.func_name
    logging.info(u'写入的日志文件为:%s', logfile)
    report = Report()
    report.statistic_db('2014-08-11', '2014-08-12')
    logging.info(u'程序运行完成')
