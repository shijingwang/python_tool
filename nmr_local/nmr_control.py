# -*- coding: utf-8 -*-
from common.con_util import ConUtil
import settings
import logging
import time
import traceback
import os
import datetime
from nmr_extract import Nmr
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

class NmrControl(object):
    
    def __init__(self):
        self.db_molbase = ConUtil.connect_mysql(settings.MYSQL_MOLBASE)
        self.chemacx_extract = Nmr()
    
    def load_data(self):
        counter = 0;
        while True:
            counter += 1
            if counter > 10000:
                logging.info("循环次数过多，退出")
                break
            sql = "select * from mark where type=100"
            data = self.db_molbase.query(sql)
            if len(data) == 0:
                qsql = 'select * from search_moldata order by mol_id asc limit 1000'
            else:
                d = data[0]
                qsql = 'select * from search_moldata where mol_id>%s order by mol_id asc limit 1000' % d['value']
            data = self.db_molbase.query(qsql)
            for d in data:
                if d['cas_no'] == None:
                    continue
                if d['formula'] == None:
                    continue
                if not 'H' in d['formula']:
                    continue
                self.generate_pic(d['mol_id'], d['cas_no'])
                #break
            #break
    
    def generate_pic(self, mol_id, cas_no):
        struc_sql = 'select * from search_molstruc where mol_id=%s' % mol_id
        try:
            sdata = self.db_molbase.query(struc_sql)
            if len(sdata) == 0:
                raise Exception(u'没有Mol数据:%s' % mol_id, 555)
            sd = sdata[0]
            mol_path = str(settings.MOL_FILE_PATH + '%s.mol' % mol_id)
            mol_path = mol_path.replace("/", "\\")
            write_mol = open(mol_path, 'wb')
            write_mol.write(sd['struc'])
            write_mol.close()
            days = datetime.datetime.now().strftime('%Y-%m-%d')
            pic_path = settings.NMR_PIC_PATH + days + '/' + storage_dir(mol_id) + '/'
            if not os.path.exists(pic_path):
                os.makedirs(pic_path)
            pic_path = pic_path.replace("/", "\\")
            pic_1h = str(pic_path + cas_no + '-1h.png')
            pic_13c = str(pic_path + cas_no + '-13c.png')
            self.delete_file(pic_1h)
            self.delete_file(pic_13c)
            self.nmr.opchemacx_extractmol(mol_path)
            self.nmr.gechemacx_extractate_1h_image(pic_1h)
            time.sleep(1)
            self.nmr.gechemacx_extractate_13c_image(pic_13c)
            time.sleep(1)
            self.nmr.clchemacx_extract_mol()
            time.sleep(1)
            usql = 'update search_moldata set nmr_sf_status=1 where mol_id=%s' % mol_id
            self.db_molbase.update(usql)
        except Exception, e:
            self.nmr.fichemacx_extractstop()
            self.nmr.stchemacx_extractup_app()
            usql = 'update search_moldata set nmr_sf_status=2 where mol_id=%s' % mol_id
            self.db_molbase.update(usql)
            logging.error(u'生成mol_id:%s 核磁数据出错', mol_id, e);
            logging.error(traceback.format_exc())
        finally:
            self.delete_file(mol_path)
            msql = 'insert into mark (type, value) values (100,%s) on duplicate key update value=%s' % (mol_id, mol_id)
            self.db_molbase.update(msql)

    def delete_file(self,fp):
        try:
            if os.path.exists(fp):
                os.remove(fp)
        except Exception:
            pass
        
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')

    define("logfile", default="D:/Log/nmr_sf.log", help="NSQ topic")
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
    nc = NmrControl()
    nc.load_data()
