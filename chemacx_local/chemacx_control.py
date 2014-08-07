# -*- coding: utf-8 -*-
from common.con_util import ConUtil
import settings
import logging
import time
import traceback
import os
import datetime
from chemacx_local.chemacx_extract import ChemacxExtract
from tornado.options import define, options

def storage_dir(sdir):
        sdir = str(sdir)
        if len(sdir) < 9:
            while True:
                sdir = '0' + sdir
                if len(sdir) == 9:
                    break
        else:
            sdir = sdir[0:9]
        return sdir[0:3] + '/' + sdir[3:6] + '/' + sdir[6:9] 

class ChemacxControl(object):
    
    def __init__(self):
        self.db_molbase = ConUtil.connect_mysql(settings.MYSQL_MOLBASE)
        self.chemacx_extract = ChemacxExtract()
    
    def load_data(self):
        counter = 0;
        while True:
            counter += 1
            if counter > 10000:
                logging.info("循环次数过多，退出")
                break
            sql = "select * from mark where type=200"
            data = self.db_molbase.query(sql)
            if len(data) == 0:
                qsql = 'select * from chemacx order by id asc limit 1000'
            else:
                d = data[0]
                qsql = 'select * from chemacx where id>%s order by id asc limit 1000' % d['value']
            data = self.db_molbase.query(qsql)
            if len(data) == 0:
                break
            for d in data:
                if d['acx_number'] == None or len(d['acx_number'].strip()) == 0:
                    continue
                if d['cas'] == None or len(d['cas'].strip()) == 0:
                    continue
                self.generate_pic(d['id'], d['acx_number'], d['cas'])
    
    def generate_pic(self, cid, acx_number, cas_no):
        try:
            days = datetime.datetime.now().strftime('%Y-%m-%d')
            mol_file_path = settings.CHEM_ACX_MOLFILE_PATH + days + '/'
            if not os.path.exists(mol_file_path):
                os.makedirs(mol_file_path)
            mol_file_path = mol_file_path.replace("/", "\\")
            mol_file_path = str(mol_file_path + cas_no + '.mol')
            self.delete_file(mol_file_path)
            self.chemacx_extract.entry_acx_number(str(acx_number))
            time.sleep(3)
            self.chemacx_extract.save_to_mol(mol_file_path)
            self.chemacx_extract.close_document()
            self.chemacx_extract.new_blank_document()
            usql = 'update chemacx set status=1 where id=%s' % cid
            self.db_molbase.update(usql)
        except Exception, e:
            self.chemacx_extract.startup_app()
            usql = 'update chemacx set status=2 where id=%s' % cid
            self.db_molbase.update(usql)
            logging.error(u'生成id:%s mol数据出错', cid, e);
            logging.error(traceback.format_exc())
        finally:
            self.delete_file(cid)
            msql = 'insert into mark (type, value) values (200,%s) on duplicate key update value=%s' % (cid, cid)
            self.db_molbase.update(msql)

    def delete_file(self, fp):
        try:
            if os.path.exists(fp):
                os.remove(fp)
        except Exception:
            pass
        
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')

    define("logfile", default="D:/Log/chemacx.log", help="NSQ topic")
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
    chemacxControl = ChemacxControl()
    chemacxControl.load_data()
