# -*- coding: utf-8 -*-
from tornado.options import define, options
import logging
import os, sys
import traceback
import json
try:
    import python_tool
except ImportError:
    fp = os.path.abspath(__file__)
    sys.path.append(fp[0:fp.rfind('python_tool') + 11])
from common.con_util import ConUtil
import settings

class CheckKeyFix(object):
    
    def __init__(self):
        self.db_spider_data = ConUtil.connect_mysql(settings.MYSQL_SPIDER_DATA)
        pass
    
    def check_key_fix(self):
        sql = 'select * from file_info where check_key is null'
        # file info set
        fis = self.db_spider_data.query(sql)
        logging.info(u"需要修正的记录数:%s", len(fis))
        for fi in fis:
            try:
                fp = settings.MSDS_FILE_PATH + fi['path']
                c = 'md5sum  %s' % fp
                logging.info(u'执行Linux指令:%s', c)
                f_md5 = os.popen(c).read()
                f_md5 = f_md5.replace('\n', '').replace('\r', '')
                f_md5 = f_md5[:f_md5.find(fp)].strip()
                if len(f_md5) != 32:
                    continue
                sql = "update file_info set check_key='%s' where id=%s"
                sql = sql % (f_md5, fi['id'])
                self.db_spider_data.execute(sql)
            except Exception, e:
                logging.info(traceback.format_exc())
        return 1

if __name__ == '__main__':

    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="/tmp/check_key_fix.log", help="NSQ topic")
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
    ed = CheckKeyFix()
    ed.check_key_fix()
    logging.info(u'程序运行完成')
