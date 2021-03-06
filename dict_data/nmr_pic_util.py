# -*- coding: utf-8 -*-
from tornado.options import define, options
import logging
import os, sys, time
import traceback
import json, base64
import threading
import re
from PIL import Image
try:
    import python_tool
except ImportError:
    fp = os.path.abspath(__file__)
    sys.path.append(fp[0:fp.rfind('python_tool') + 11])
import dict_conf
from common.con_util import ConUtil
import CK
import settings

class NmrPicUtil(object):
    
    def __init__(self):
        self.redis_server = ConUtil.connect_redis(dict_conf.REDIS_SERVER)
        self.db_molbase = ConUtil.connect_mysql(settings.MYSQL_MOLBASE)
    
    def img_import_msg(self):
        msg = {'mol_id':1}
        msg['cas'] = '12311-11-0'
        pic_dict = {'1h':'/home/kulen/Pictures/2.jpg', '13c':'/home/kulen/Pictures/3.jpg'}
        for key in pic_dict:
            img_reader = open(pic_dict[key], 'rb')
            v_img = img_reader.read()
            img_reader.close()
            im = Image.open(pic_dict[key])
            width, height = im.size
            e_img = base64.encodestring(v_img)
            pic_info = {}
            pic_info['img'] = e_img
            pic_info['width'] = width
            pic_info['height'] = height
            msg[key] = pic_info 
            
        msg_j = json.dumps(msg)
        self.redis_server.lpush(CK.R_NMR_IMPORT, msg_j)
    
    def mol_import_msg(self):
        sql = "select * from search_moldata order by mol_id asc limit 10"
        rs = self.db_molbase.query(sql)
        for r in rs:
            logging.info("导入mol_id:%s,cas_no:%s 的mol数据", r['mol_id'], r['cas_no'])
            sql = "select * from search_molstruc where mol_id=%s"
            srs = self.db_molbase.query(sql, r['mol_id'])
            for sr in srs:
                msg = {}
                msg['mol_id'] = r['mol_id']
                msg['cas_no'] = r['cas_no']
                msg['mol'] = sr['struc']
                msg_j = json.dumps(msg)
                self.redis_server.lpush(CK.R_NMR_CREATE, msg_j)
                break
        pass
        
if __name__ == '__main__':

    reload(sys)
    sys.setdefaultencoding('utf-8')
    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="/tmp/nmr_pic_util.log", help="NSQ topic")
    options.parse_command_line()
    logfile = options.logfile
    fmt = '%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s'
    handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=100 * 1024 * 1024, backupCount=10)  # 实例化handler
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt)  # 实例化formatter
    handler.setFormatter(formatter)  # 为handler添加formatter
    logging.getLogger('').addHandler(handler)
    logging.info(u'写入的日志文件为:%s', logfile)
    
    npu = NmrPicUtil()
    # npu.img_import_msg()
    npu.mol_import_msg()
    logging.info(u'程序运行完成')
