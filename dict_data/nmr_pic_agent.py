# -*- coding: utf-8 -*-
from tornado.options import define, options
import logging
import os, sys, time, datetime
import traceback
import json, base64
try:
    import python_tool
except ImportError:
    fp = os.path.abspath(__file__)
    sys.path.append(fp[0:fp.rfind('python_tool') + 11])
import dict_conf
from common.con_util import ConUtil
import CK

class NmrPicAgent(object):
    
    def __init__(self):
        self.redis_server = ConUtil.connect_redis(dict_conf.REDIS_SERVER)
        self.db_dict = ConUtil.connect_mysql(dict_conf.MYSQL_DICT_AGENT)
    
    def import_nmr_pic(self):
        msg = self.redis_server.rpop(CK.R_NMR_IMPORT)
        if not msg:
            logging.warn(u"消息内容为空!")
            return
        msg_j = json.loads(msg)
        mol_id = msg_j['mol_id']
        cas = msg_j['cas']
        logging.info(u"处理mol_id:%s cas:%s NMR图片数据", mol_id, cas)
        sql = 'select * from search_nmr where mol_id=%s' % mol_id
        rs = self.db_dict.query(sql)
        logging.info(u'数据库中找出的NMR的数量:%s', len(rs))
        for key in ['1h', '13c']:
            if key not in msg_j:
                continue
            logging.info(u'处理Key:%s 数据', key)
            try:
                #  insert type
                itype = 1 if key == '1h' else 2
                for r in rs:
                    del_file_path = dict_conf.agent_nmr_picdir + r['path']
                    if r['type'] == itype:
                        logging.info("mol_id:%s path:%s", r['mol_id'], r['path'])
                        sql = 'delete from search_nmr where id=%s' % r['id']
                        logging.info(u'执行的sql:%s' , sql)
                        self.db_dict.execute(sql)
                        self.delete_file(mol_id, del_file_path)
                # 对文件数据进行写入
                days = datetime.datetime.now().strftime('%Y-%m-%d')
                relative_path = '/%s/%s' % (days, self.generate_pic_path(mol_id, key))
                pic_fp = dict_conf.agent_nmr_picdir + relative_path
                if not os.path.exists(pic_fp[0:pic_fp.rfind('/')]):
                    os.makedirs(pic_fp[0:pic_fp.rfind('/')])
                img_writer = open(pic_fp, 'wb')
                img_writer.write(base64.decodestring(msg_j[key]['img']))
                img_writer.close()
                sql = 'insert into search_nmr (mol_id,type,cas,path,width,height) values (%s,%s,%s,%s,%s,%s)'
                self.db_dict.execute(sql, mol_id, itype, cas, relative_path, msg_j[key]['width'], msg_j[key]['height'])
            except Exception, e:
                logging.error(u"处理mol_id:%s key:%s 出错", mol_id, key)
                logging.error(traceback.format_exc())
    
    def import_nmr_pic_thread(self):
        logging.info(u'启动字典数据写入线程')
        while True:
            try:
                size = self.redis_server.llen(CK.R_NMR_IMPORT)
                logging.info(u'NMRImport队列中的数据大小为:%s', size)
                if size == 0:
                    time.sleep(3)
                    continue
                self.import_nmr_pic()
            except Exception, e:
                time.sleep(3)
                logging.error(u"NMRImport队列中的数据处理出错:%s", e)
                logging.error(traceback.format_exc())
    
    def generate_pic_path(self, mol_id, ftype):
        pic_path = str(mol_id)
        while len(pic_path) < 9:
            pic_path = '0' + pic_path
        pic_dir = pic_path[0:6]
        pic_dir = '%s/%s/%s_%s.png' % (pic_dir[0:3], pic_dir[3:6], mol_id, ftype)
        return pic_dir
    
    def delete_file(self, mol_id, fp):
        logging.info(u'删除mol_id:%s 文件:%s', mol_id, fp)
        try:
            if os.path.exists(fp):
                os.remove(fp)
        except Exception:
            pass

        
if __name__ == '__main__':

    reload(sys)
    sys.setdefaultencoding('utf-8')
    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="/tmp/dict_agent.log", help="NSQ topic")
    options.parse_command_line()
    logfile = options.logfile
    fmt = '%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s'
    handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=100 * 1024 * 1024, backupCount=10)  # 实例化handler
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt)  # 实例化formatter
    handler.setFormatter(formatter)  # 为handler添加formatter
    logging.getLogger('').addHandler(handler)
    logging.info(u'写入的日志文件为:%s', logfile)
    npa = NmrPicAgent()
    npa.import_nmr_pic_thread()
    # npa.import_nmr_pic()
    logging.info(u'程序运行完成')
