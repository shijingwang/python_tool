# -*- coding: utf-8 -*-
import json, base64
import logging
import os, sys, time
from PIL import Image
from tornado.options import define, options
import traceback
try:
    import python_tool
except ImportError:
    fp = os.path.abspath(__file__)
    sys.path.append(fp[0:fp.rfind('python_tool') + 11])
from common.con_util import ConUtil
import dict_conf
import CK
from nmr_local.nmr_extract import Nmr

# 解决worker过程中无法删除的一些图片给删除
class NmrPicDelete(object):
    
    def __init__(self):
        pass

    def delete_expire_pic(self):
        logging.info(u"检查有没有文件需要删除")
        files = os.listdir(dict_conf.chemdraw_work_dir)
        for tfile in files:
            try:
                tfile = "%s\\%s" % (dict_conf.chemdraw_work_dir, tfile)
                if not os.path.isfile(tfile):
                    continue
                ctime = os.path.getctime(tfile)
                ctime = int(ctime)
                ntime = int(time.time())
                if ntime - ctime >= 300:
                    logging.info(u"删除文件:%s", tfile)
                    os.remove(tfile)
            except Exception, e:
                logging.error(u'处理文件:%s 出错, %s', tfile, e);
                logging.error(traceback.format_exc())
    
    def delete_file_pic_thread(self):
        while True:
            try:
                time.sleep(3)
                self.delete_expire_pic()
            except Exception, e:
                logging.error(traceback.format_exc())
    
if __name__ == '__main__':

    reload(sys)
    sys.setdefaultencoding('utf-8')
    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="c:\\nmr_worker.log", help="NSQ topic")
    options.parse_command_line()
    logfile = options.logfile
    fmt = '%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s'
    handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=100 * 1024 * 1024, backupCount=10)  # 实例化handler
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt)  # 实例化formatter
    handler.setFormatter(formatter)  # 为handler添加formatter
    logging.getLogger('').addHandler(handler)
    logging.info(u'写入的日志文件为:%s', logfile)
    npw = NmrPicDelete()
    # npw.nmr_create_task()
    npw.delete_file_pic_thread()
    # os.remove("C:/ChemDraw/mark_44-1h.png");
    # npw.resize_pic()
    logging.info(u'程序运行完成')
