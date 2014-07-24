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

class MolCheck(object):
    
    def __init__(self):
        self.dict_db = ConUtil.connect_mysql(settings.MYSQL_DICT)
        self.dict_source_db = ConUtil.connect_mysql(settings.MYSQL_DICT_SOURCE)
        self.p = re.compile(r"([0-9]{2,7})[-]{1}([0-9]{2})[-]{1}([0-9]{1})$")
    
    def check_search_data(self):
        counter = 0
        size = 1000
        check_dir = '/tmp/mol_check'
        if not os.path.exists(check_dir):
            os.makedirs(check_dir)
        while True:
            counter += 1
            if counter >= 100000:
                logging.info(u'循环次数过多,退出')
                break
            logging.info(u"进行第%s轮查询", counter)
            try:
                sql = 'select * from mark where type=2'
                # mark data set
                mds = self.dict_source_db.query(sql)
                # mv mark value
                if len(mds) == 0:
                    mv = '0'
                else:
                    mv = mds[0]['value']
                sql = 'select * from etlv2 where id>%s order by id asc limit %s'
                sql = sql % (mv, size)
                logging.info(u"sql语句为:%s", sql)
                # etl data set
                eds = self.dict_source_db.query(sql)
                if len(eds) == 0:
                    break
                for ed in eds:
                    try:
                        # check pubchem  smile inchi 生成的mol文件是否匹配
                        c = "echo \"%s\"|babel -iinchi -omol --gen2d>/tmp/mol_check/%s-i.mol"
                        logging.info(u'执行指令:%s', c)
                        result = os.popen(c).read()
                        logging.info(result)
                        c = "echo \"%s\"|babel -ican -omol --gen2d>/tmp/mol_check/%s-c.mol"
                        logging.info(u'执行指令:%s', c)
                        result = os.popen(c).read()
                        logging.info(result)
                        c = '%s -aixsgG %s %s'
                        logging.info(u'执行Linux指令:%s', c)
                        break
                    except Exception, e:
                        logging.error(u'验证CAS号:%s 的mol数据时出错', ed['cas'])
                        logging.error(traceback.format_exc())
                    break
                    sql = "insert into mark (type,value) values (2, '%s') on duplicate key update value='%s'"
                    sql = sql % (ed['mol_id'], ed['mol_id'])
                    self.dict_source_db.execute(sql)
            except Exception, e:
                logging.error(u'提取数据时出错:%s', e)
                logging.error(traceback.format_exc())
            break

if __name__ == '__main__':

    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="/tmp/mol_check.log", help="NSQ topic")
    define("func_name", default="check_search_mol")
    options.parse_command_line()
    logfile = options.logfile
    fmt = '%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s'
    handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=100 * 1024 * 1024, backupCount=10)  # 实例化handler
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt)  # 实例化formatter
    handler.setFormatter(formatter)  # 为handler添加formatter
    logging.getLogger('').addHandler(handler)
    logging.info(u'写入的日志文件为:%s', logfile)
    mc = MolCheck()
    mc.check_search_data()
    logging.info(u'程序运行完成')