# -*- coding: utf-8 -*-
from common.con_util import ConUtil
import settings
import logging
import time
import traceback
from nmrdb import Nmrdb
from tornado.options import define, options

class NmrdbControl(object):
    
    def __init__(self):
        self.db_molbase = ConUtil.connect_mysql(settings.MYSQL_MOLBASE)
    
    def spider_all_data(self):
        counter = 0
        while True:
            counter += 1
            logging.info(u'第%s轮数据抓取', counter)
            sql = 'select * from mark where type=1'
            data = self.db_molbase.query(sql)
            if len(data) == 0:
                qsql = 'select * from cas_10w_clean order by mol_id asc limit 1000'
            else:
                d = data[0]
                qsql = 'select * from cas_10w_clean where mol_id>%s order by mol_id asc limit 1000' % d['value']
            qdata = self.db_molbase.query(qsql)
            if len(qdata) == 0:
                break
            if counter > 10000:
                break
            mol_ids = []
            for d in qdata:
                mol_ids.append(str(d['mol_id']))
            self.import_data(mol_ids, '1')
            
        
    
    def import_data(self, mol_ids, update_type):
        nmrdb_v = Nmrdb()
        mol_ids = str(mol_ids)
        mol_ids = mol_ids[1:len(mol_ids) - 1]
        # print mol_ids
        sql = "select * from search_moldata where mol_id in (" + mol_ids + ") and formula like '%%H%%' and nmr_status=0"
        data = self.db_molbase.query(sql)
        logging.info(u"查询到的数据为:%s", len(data))
        counter = 0;
        for d in data:
            counter += 1
            struc_sql = 'select * from search_molstruc where mol_id=%s' % (d['mol_id'])
            sdata = self.db_molbase.query(struc_sql)
            sd = sdata[0]
            process_v = sd['struc']
            process_v = process_v[process_v.index('\n\n') + 2:]
            # print process_v
            read_object = open(settings.APP_PATH + 'predictor_template.htm')
            try:
                all_the_text = read_object.read()
            finally:
                read_object.close()
            # print all_the_text;
            
            all_the_text = all_the_text.replace('importmolfile', process_v)
            write_object = open(settings.APP_PATH + 'predictor.htm', 'wb')
            try:
                write_object.write(all_the_text)
            finally:
                write_object.close()
            try:
                nmrdb_v.download_nmr(d['cas_no'])
                usql = 'update search_moldata set nmr_status=1 where mol_id=%s' % (d['mol_id'])
                self.db_molbase.update(usql)
            except Exception, e:
                usql = 'update search_moldata set nmr_status=2 where mol_id=%s' % (d['mol_id'])
                self.db_molbase.update(usql)
                logging.error(u'抓取CAS:%s 核磁共振数据:%s', d['cas_no'], e);
                logging.error(traceback.format_exc())
            finally:
                msql = 'insert into mark (type, value) values (%s,%s) on duplicate key update value=%s' % (update_type, d['mol_id'], d['mol_id'])
                self.db_molbase.update(msql)

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')

    define("logfile", default="F:/Log/py.log", help="NSQ topic")
    options.parse_command_line()
    logfile = options.logfile
    logging.info(u'写入的日志文件为:%s', logfile)
    # 自动对日志文件进行分割
    fmt = '%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s'
    handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=100 * 1024 * 1024, backupCount=10)  # 实例化handler
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt)  # 实例化formatter
    handler.setFormatter(formatter)  # 为handler添加formatter
    logging.getLogger('').addHandler(handler)
    nc = NmrdbControl()
    nc.spider_all_data()
    
