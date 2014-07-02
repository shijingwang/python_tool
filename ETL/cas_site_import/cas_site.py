# -*- coding: utf-8 -*-

from tornado.options import define, options
from tornado import escape
import logging
import urllib
import urllib2
import csv
import sys
import os

class CasSite(object):
    
    def write_cas_data(self):
        logging.info("开始读取")
        f = open('/home/kulen/Documents/cas_site.csv', 'rb')
        reader = csv.reader(f)
        counter = 0
        for row in reader:
            counter += 1
            if counter == 1:
                continue
            while True:
                try:
                    logging.info("ID:%s Name:%s URL:%s", row[0], row[1], row[2])
                    param = {
                             'v_id':row[0],
                             'name':row[1],
                             'url':row[2]
                             }
                    url = 'http://122.225.18.10:3000/cas/insert?' + urllib.urlencode(param) 
                    req = urllib2.Request(url)
                    response = urllib2.urlopen(req, timeout=3)
                    _html = response.read()
                    logging.info("返回的内容:" + _html)
                    break;
                except Exception, e:
                    logging.error(e)
                finally:
                    try:
                        response.close()
                        logging.info("关闭连接")
                    except Exception:
                        pass
        logging.info("完成数据读取")

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
    
    
