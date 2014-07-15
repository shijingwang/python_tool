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

class DictCheck(object):
    
    def __init__(self):
        self.dict_db = ConUtil.connect_mysql(settings.MYSQL_DICT)
        self.dict_source_db = ConUtil.connect_mysql(settings.MYSQL_DICT_SOURCE)
        self.p = re.compile(r"([0-9]{2,7})[-]{1}([0-9]{2})[-]{1}([0-9]{1})$")
    
    def data_check(self):
        counter = 0
        size = 1000
        while True:
            counter += 1
            if counter >= 100000:
                logging.info(u'循环次数过多,退出')
                break
            logging.info(u"进行第%s轮查询", counter)
            try:
                sql = 'select * from mark where type=1'
                # mark data set
                mds = self.dict_source_db.query(sql)
                if len(mds) == 0:
                    mv = '0'
                else:
                    mv = mds[0]['value']
                sql = 'select * from pubchem_82w where id>%s order by id asc limit %s'
                sql = sql % (mv, size)
                logging.info(u"sql语句为:%s", sql)
                # cas data set
                cds = self.dict_source_db.query(sql)
                if len(cds) == 0:
                    break
                for cd in cds:
                    if not self.p.match(cd['cas']):
                        logging.warn(u"源数据CAS不符合规则:%s", cd['cas'])
                        continue
                    sql = "select * from search_moldata where cas_no='%s'"
                    sql = sql % cd['cas']
                    logging.info(u"Cas:%s", cd['cas'])
                    # dict cas set
                    dcs = self.dict_db.query(sql)
                    if len(dcs) == 0:
                        sql = "insert into etl (cas,diff_type) values ('%s', 1)"
                        sql = sql % (cd['cas'])
                        self.dict_source_db.execute(sql)
                    else:
                        # dict cas set
                        dc = dcs[0]
                        if dc['smiles'] != cd['Canonical_SMILES'] or dc['inchi'] != cd['InChI']:
                            smile_match = 0 if dc['smiles'] == cd['Canonical_SMILES'] else 1
                            inchi_match = 0 if dc['inchi'] == cd['InChI'] else 1
                            sql = "insert into etl (cas,diff_type,smile_match,source_smile,target_smile,inchi_match,source_inchi,target_inchi) values('%s',2,%s,'%s','%s',%s,'%s','%s')"
                            sql = sql % (cd['cas'], smile_match, cd['Canonical_SMILES'], dc['smiles'], inchi_match, cd['InChI'], dc['inchi'])
                            self.dict_source_db.execute(sql)
                    sql = "insert into mark (type,value) values (1, '%s') on duplicate key update value='%s'"
                    sql = sql % (cd['id'], cd['id'])
                    self.dict_source_db.execute(sql)
            except Exception, e:
                logging.error(u'提取数据时出错:%s', e)
                logging.error(traceback.format_exc())     

if __name__ == '__main__':

    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="/tmp/dict_check.log", help="NSQ topic")
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
    dc = DictCheck()
    dc.data_check()
    logging.info(u'程序运行完成')
