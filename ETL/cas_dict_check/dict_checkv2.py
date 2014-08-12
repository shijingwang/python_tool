# -*- coding: utf-8 -*-
import logging
from tornado.options import define, options
import traceback
import re
import os, sys
try:
    import python_tool
except ImportError:
    fp = os.path.abspath(__file__)
    sys.path.append(fp[0:fp.rfind('python_tool') + 11])
from common.con_util import ConUtil
import settings

class DictCheckV2(object):
    
    def __init__(self):
        self.dict_db = ConUtil.connect_mysql(settings.MYSQL_DICT)
        self.dict_source_db = ConUtil.connect_mysql(settings.MYSQL_DICT_SOURCE)
        self.p = re.compile(r"([0-9]{2,7})[-]{1}([0-9]{2})[-]{1}([0-9]{1})$")
    
    def data_check(self):

        sql = 'select cas,Canonical_SMILES,InChI from pubchem_82w'
        logging.info(u"sql语句为:%s", sql)
        # source data set
        sds = self.dict_source_db.query(sql)
        logging.info(u'源记录有:%s', len(sds))
        
        sql = 'select cas_no,smiles,inchi from search_moldata'
        # dict data set
        dds = self.dict_db.query(sql)
        logging.info(u'字典记录有:%s', len(dds))
        
        sdc = {}
        ddc = {}
        for sd in sds:
            # 去除CAS号不合法的数据
            if not self.p.match(sd['cas']):
                logging.warn(u"源数据CAS不符合规则:%s", sd['cas'])
                continue
            sdc[sd['cas']] = sd
        
        for dd in dds:
            ddc[dd['cas_no']] = dd
        
        counter = 0
        isql = "insert into etlv2 (cas,diff_type) values (%s, 1) on duplicate key update diff_type=1"
        usql = "insert into etlv2 (cas,diff_type,smile_match,source_smile,target_smile,inchi_match,source_inchi,target_inchi) values(%s,2,%s,%s,%s,%s,%s,%s) on duplicate key update diff_type=2"
        isqls = []
        usqls = []
        for key in sdc.keys():
            counter += 1
            if counter % 1000 == 0:
                logging.info(u'当前处理的记录量:%s', counter)
            if  key not in ddc:
                isqls.append((key,))
            else:
                # source data
                sd = sdc[key]
                # dict data
                dd = ddc[key]
                try:
                    if not dd['inchi'].startswith('InChI='):
                        dd['inchi'] = 'InChI=' + dd['inchi']
                    if dd['smiles'] != sd['Canonical_SMILES'] or dd['inchi'] != sd['InChI']:
                        smile_match = 0 if dd['smiles'] == sd['Canonical_SMILES'] else 1
                        inchi_match = 0 if dd['inchi'] == sd['InChI'] else 1
                        usqls.append((sd['cas'], smile_match, sd['Canonical_SMILES'], dd['smiles'], inchi_match, sd['InChI'], dd['inchi']))
                except Exception, e:
                    logging.error(u'写入数据时出错:%s', e)
                    logging.error(traceback.format_exc()) 
            if len(isqls) >= 1000:
                self.dict_source_db.insertmany(isql, isqls)
                isqls = []
            if len(usqls) >= 1000:
                self.dict_source_db.insertmany(usql, usqls)
                usqls = []
        
    def test_method(self):
        isql = "insert into etlv2 (cas,diff_type) values (%s, 1) on duplicate key update diff_type=1"
        isqls = []
        isqls.append(tuple('1'))
        isqls.append(tuple('2'))
        isqls.append(tuple('3'))
        print isqls
        self.dict_source_db.insertmany(isql, isqls)
        isqls = []

if __name__ == '__main__':

    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="/tmp/dict_checkv2.log", help="NSQ topic")
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
    dc = DictCheckV2()
    dc.data_check()
    logging.info(u'程序运行完成')
