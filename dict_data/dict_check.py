# -*- coding: utf-8 -*-
import logging
import os, sys
import time
from tornado.options import define, options
import traceback
try:
    import python_tool
except ImportError:
    fp = os.path.abspath(__file__)
    sys.path.append(fp[0:fp.rfind('python_tool') + 11])

from common.con_util import ConUtil
import dict_conf

class DictCheck(object):
    
    def __init__(self):
        self.db_dict = ConUtil.connect_mysql(dict_conf.MYSQL_DICT_WORKER)

    def data_check(self):
        data_set = self.get_data_set('search_moldata')
        struc_set = self.get_data_set('search_molstruc')
        pic_set = self.get_data_set('search_pic2d')
        cfp_set = self.get_data_set('search_molcfp')
        fgb_set = self.get_data_set('search_molfgb')
        stat_set = self.get_data_set('search_molstat')
        logging.info('moldata size:%s molstruc size:%s pic size:%s', len(data_set), len(struc_set), len(pic_set))
        logging.info('molcfp size:%s molfgb size:%s molstat size:%s', len(cfp_set), len(fgb_set), len(stat_set))
        
        struc_dif_set = data_set - struc_set
        logging.info(u'struc表缺少的数据 size:%s content:%s', len(struc_dif_set), struc_dif_set)
        cfp_dif_set = data_set - cfp_set
        logging.info(u'cfp表缺少的数据 size:%s content:%s', len(cfp_dif_set), cfp_dif_set)
        fgb_dif_set = data_set - fgb_set
        logging.info(u'fgb表缺少的数据 size:%s content:%s', len(fgb_dif_set), fgb_dif_set)
        stat_dif_set = data_set - stat_set
        logging.info(u'stat表缺少的数据 size:%s content:%s', len(stat_dif_set), stat_dif_set)
        
        data_dif_set = struc_set - data_set
        logging.info(u'data表缺少的数据 size:%s content:%s', len(data_dif_set), data_dif_set)
        
    def get_data_set(self, table_name):
        logging.info(u'查询表:%s 数据', table_name)
        sql = 'select mol_id from ' + table_name
        rs = self.db_dict.query(sql)
        dataset = set()
        for r in rs:
            dataset.add(r['mol_id'])
        return dataset

if __name__ == '__main__':

    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="/tmp/dict_check.log", help="NSQ topic")
    define("func_name", default="import_table_data")
    options.parse_command_line()
    logfile = options.logfile
    func_name = options.func_name
    fmt = '%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s'
    handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=100 * 1024 * 1024, backupCount=10)  # 实例化handler
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt)  # 实例化formatter
    handler.setFormatter(formatter)  # 为handler添加formatter
    logging.getLogger('').addHandler(handler)
    logging.info(u'写入的日志文件为:%s', logfile)
    dictCheck = DictCheck()
    dictCheck.data_check()
    logging.info(u'程序运行完成')
