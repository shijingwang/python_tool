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
        usql = "insert into etlv3 (cas,diff_type,smile_match,source_smile,target_smile,inchi_match,source_inchi,target_inchi) values(%s,%s,%s,%s,%s,%s,%s,%s) on duplicate key update diff_type=2"
        usqls = []
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
                        logging.info(u'处理编号:%s cas号:%s 的数据!', ed['id'], ed['cas'])
                        if ed['diff_type'] == 1:
                            raise Exception(u"不匹配的类型", 501)
                        # check pubchem  smile inchi 生成的mol文件是否匹配
                        mol_1 = "/tmp/mol_check/%s-i.mol" % ed['cas']  # -i inchi
                        mol_2 = "/tmp/mol_check/%s-c.mol" % ed['cas']  # -c canonical smile
                        mol_3 = "/tmp/mol_check/%s-dc.mol" % ed['cas']  # -dc dict canonical smile
                        self.delete_file(mol_1)
                        self.delete_file(mol_2)
                        self.delete_file(mol_3)
                        c = "echo \"%s\"|babel -iinchi -omol --gen2d>%s" % (ed['source_inchi'], mol_1)
                        logging.info(u'执行指令:%s', c)
                        result = os.popen(c).read()
                        # logging.info(result)
                        c = "echo \"%s\"|babel -ican -omol --gen2d>%s" % (ed['source_smile'], mol_2)
                        logging.info(u'执行指令:%s', c)
                        result = os.popen(c).read()
                        # logging.info(result)
                        c = '%s -aixsgG %s %s' % (settings.MATCH_MOL_CMD, mol_1, mol_2)
                        logging.info(u'执行Linux指令:%s', c)
                        result = os.popen(c).read()
                        if result != None:
                            result = result.replace('\n', '').strip()
                        # pubchem smile inchi 匹配成功
                        logging.info(u"匹配结果:[%s]", result)
                        if result != None and result == '1:T':
                            pass
                        else:
                            usqls.append((ed['cas'], 1, ed['smile_match'], ed['source_smile'], ed['target_smile'], ed['inchi_match'], ed['source_inchi'], ed['target_inchi']))
                            logging.info(u"CAS:%s pubchem数据源生成的mol文件不匹配", ed['cas'])
                            raise Exception(u"Pubchem Mol文件不匹配", 502)
                        # pubchem 和 moldata　生成的mol进行匹配，如果匹配成功，则不需要进行更新，否则对mol的数据进行更新
                        c = "echo \"%s\"|babel -ican -omol --gen2d>%s" % (ed['target_smile'], mol_3)
                        logging.info(u'执行指令:%s', c)
                        result = os.popen(c).read()
                        c = '%s -aixsgG %s %s' % (settings.MATCH_MOL_CMD, mol_1, mol_3)
                        logging.info(u'执行Linux指令:%s', c)
                        result = os.popen(c).read()
                        if result != None:
                            result = result.replace('\n', '').strip()
                        logging.info(u"匹配结果:[%s]", result)
                        # pubchem smile inchi 匹配成功
                        if result != None and result == '1:T':
                            usqls.append((ed['cas'], 2, ed['smile_match'], ed['source_smile'], ed['target_smile'], ed['inchi_match'], ed['source_inchi'], ed['target_inchi']))
                            logging.info(u"CAS:%s pubchem 和字典数据匹配", ed['cas'])
                        else:
                            usqls.append((ed['cas'], 3, ed['smile_match'], ed['source_smile'], ed['target_smile'], ed['inchi_match'], ed['source_inchi'], ed['target_inchi']))
                            logging.info(u"CAS:%s pubchem 和字典数据匹配不成功", ed['cas'])
                    except Exception, e:
                        logging.error(u'验证CAS号:%s 的mol数据时出错', ed['cas'])
                        logging.error(traceback.format_exc())
                    finally:
                        self.delete_file(mol_1)
                        self.delete_file(mol_2)
                        self.delete_file(mol_3)
                    sql = "insert into mark (type,value) values (2, '%s') on duplicate key update value='%s'"
                    sql = sql % (ed['id'], ed['id'])
                    self.dict_source_db.execute(sql)
                if len(usqls) > 0:
                    self.dict_source_db.insertmany(usql, usqls)
                    usqls = []
            except Exception, e:
                logging.error(u'提取数据时出错:%s', e)
                logging.error(traceback.format_exc())
    
    def delete_file(self, fp):
        try:
            if os.path.exists(fp):
                os.remove(fp)
        except Exception:
            pass

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
