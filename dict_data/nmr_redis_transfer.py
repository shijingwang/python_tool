# -*- coding: utf-8 -*-
from tornado.options import define, options
import logging
import os, sys, time
import traceback
import json, base64

try:
    import python_tool
except ImportError:
    fp = os.path.abspath(__file__)
    sys.path.append(fp[0:fp.rfind('python_tool') + 11])
import dict_conf
import CK
from common.con_util import ConUtil

class NmrRedisTransfer(object):
    
    def __init__(self):
        self.redis_server = ConUtil.connect_redis(dict_conf.REDIS_SERVER)
        self.transfer_redis_server = ConUtil.connect_redis(dict_conf.TRANSFER_REDIS_SERVER)
        self.db_dict = ConUtil.connect_mysql(dict_conf.MYSQL_DICT_WORKER)
    
    def transfer_msg(self):
        nmr_create_size = self.transfer_redis_server.llen(CK.R_NMR_CREATE)
        # 如果队列中没有数据，就向对列中写入数据
        logging.info(u'生成NMR图片任务队列大小为:%s', nmr_create_size)
        if nmr_create_size == 0:
            mark = 0
            sql = 'select * from mark where type=1'
            rs = self.db_dict.query(sql)
            if len(rs) > 0:
                for r in rs:
                    mark = r['value']
            else:
                sql = 'insert into mark (type,value) values (1,0)'
                self.db_dict.execute(sql)
            sql = 'select * from search_moldata where mol_id>%s order by mol_id asc limit 5'
            rs = self.db_dict.query(sql, mark)
            for r in rs:
                try:
                    logging.info("导入mol_id:%s,cas_no:%s 的mol数据", r['mol_id'], r['cas_no'])
                    sql = "select * from search_molstruc where mol_id=%s"
                    srs = self.db_dict.query(sql, r['mol_id'])
                    for sr in srs:
                        msg = {}
                        msg['mol_id'] = r['mol_id']
                        msg['cas_no'] = r['cas_no']
                        msg['mol'] = sr['struc']
                        msg_j = json.dumps(msg)
                        self.transfer_redis_server.lpush(CK.R_NMR_CREATE, msg_j)
                        break
                except Exception, e:
                    logging.error(u"处理mol_id:%s 的数据出错!, %s", r['mol_id'], e)
                    logging.error(traceback.format_exc())
                finally:
                    sql = 'update mark set value=%s where type=1'
                    self.db_dict.execute(sql, r['mol_id'])
        nmr_import_size = self.transfer_redis_server.llen(CK.R_NMR_IMPORT) 
        logging.info(u'需要转发的Redis的消息量:%s', nmr_import_size)
        if nmr_import_size > 0:
            counter = 0
            while True:
                counter += 1
                msg = self.transfer_redis_server.rpop(CK.R_NMR_IMPORT)
                if not msg:
                    break
                self.redis_server.lpush(CK.R_NMR_IMPORT, msg)
                if counter >= 10:
                    break

    def transfer_msg_thread(self):
        while True:
            try:
                self.transfer_msg()
            except Exception, e:
                logging.error(e)
                logging.error(traceback.format_exc())
            finally:
                time.sleep(1)

if __name__ == '__main__':

    reload(sys)
    sys.setdefaultencoding('utf-8')
    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="/tmp/redis_transfer.log", help="NSQ topic")
    options.parse_command_line()
    logfile = options.logfile
    fmt = '%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s'
    handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=100 * 1024 * 1024, backupCount=10)  # 实例化handler
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt)  # 实例化formatter
    handler.setFormatter(formatter)  # 为handler添加formatter
    logging.getLogger('').addHandler(handler)
    logging.info(u'写入的日志文件为:%s', logfile)
    
    npt = NmrRedisTransfer()
    npt.transfer_msg_thread()
    logging.info(u'程序运行完成')
