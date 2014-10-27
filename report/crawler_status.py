# -*- coding: utf-8 -*-
import csv
import logging
import sys, os
from tornado.options import define, options
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import datetime
import time
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
            result['domain'] = ''  # 设置一个默认值，防止相应的数据抓取不到
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
    
    def send_mail(self, email):
        # 加邮件头
        msg = MIMEMultipart()

        # 构造附件1
        att1 = MIMEText(open('/tmp/report.csv', 'rb').read(), 'base64', 'utf-8')
        att1["Content-Type"] = 'application/octet-stream'
        att1["Content-Disposition"] = 'attachment; filename="report.csv"'  # 这里的filename可以任意写，写什么名字，邮件中显示什么名字
        msg.attach(att1)
        msg['to'] = email
        msg['from'] = 'guoqiang.zhang@molbase.com'
        msg['subject'] = datetime.datetime.now().strftime('%Y-%m-%d') + u'抓取进度'
        # 发送邮件
        try:
            server = smtplib.SMTP()
            server.connect(settings.MAIL_SEND_SERVER)
            server.login(settings.MAIL_USER, settings.MAIL_PASSWORD)  # XXX为用户名，XXXXX为密码
            server.sendmail(msg['from'], msg['to'], msg.as_string())
            server.quit()
            logging.info("邮件发送成功")
        except Exception, e:  
            logging.error("邮件发送失败,%s", e)

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
    start_date = time.strftime('%Y-%m-%d', time.localtime(time.time() - 3600 * 24))
    stop_date = time.strftime('%Y-%m-%d', time.localtime(time.time())) 
    report.statistic_db(start_date, stop_date)
    report.send_mail("fenghao@molbase.com")
    report.send_mail("ouyangxw@molbase.com")
    report.send_mail("guoqiang.zhang@molbase.com")
    logging.info(u'程序运行完成')
