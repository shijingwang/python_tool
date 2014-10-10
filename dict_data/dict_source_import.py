# -*- coding: utf-8 -*-
from tornado.options import define, options
import logging
import os, sys
import csv
import traceback
try:
    import python_tool
except ImportError:
    fp = os.path.abspath(__file__)
    sys.path.append(fp[0:fp.rfind('python_tool') + 11])
from common.con_util import ConUtil
from common.cas_util import CasUtil
import dict_conf

class DictSourceImport(object):
    
    def __init__(self):
        self.db_dict_source = ConUtil.connect_mysql(dict_conf.MYSQL_DICT_SOURCE)
        pass
    
    # Zinc数据导入
    def readZincCsv(self):
        f = open('/home/kulen/Documents/zinc汇总数据.csv', 'rb')
        reader = csv.reader(f)
        counter = 0
        for row in reader:
            counter += 1
            if counter == 1:
                continue
            cas = ''
            if not row[1] or len(row[1].strip()) > 0:
                cas = row[1].replace('`', '')
            logging.info("%s %s %s %s", row[0], cas, row[2], row[3])
            sql = 'insert into dic_source_data (name_en,write_type,data_type,cas_no,smiles) values (%s,1,1,%s,%s)'
            self.db_dict_source.insert(sql, row[3], cas, row[2])
            if counter >= 12:
                break
        logging.info("完成数据导入")
    
    # 字典错误数据修正，导入mol文件
    def readMolFile(self):
        path = '/home/kulen/Downloads/结构式需要修正-2014-10-10/'
        for molfile in os.listdir(path):
            cas_no = molfile.replace('.mol', '')
            r_molfile = open(path + molfile, 'r')
            mol = r_molfile.read()
            sql = 'insert into dic_source_data (name_en,write_type,data_type,cas_no,mol) values (%s,2,2,%s,%s)'
            self.db_dict_source.insert(sql, '', cas_no, mol)
    
    # 字典数据修正
    def readFixCsv(self):
        f = open('/home/kulen/Downloads/要进行结构式修正的产品-2014-10-10.csv', 'r')
        reader = csv.reader(f)
        counter = 0
        for row in reader:
            counter += 1
            if counter == 1:
                continue
            sql = 'insert into dic_source_data (name_en,write_type,data_type,cas_no,inchi) values (%s,2,1,%s,%s)'
            self.db_dict_source.insert(sql, '', row[0], row[1])            

if __name__ == '__main__':

    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="/home/kulen/log/run.log", help="NSQ topic")
    define("func_name", default="spider_apple")
    options.parse_command_line()
    logfile = options.logfile
    func_name = options.func_name
    logging.info(u'写入的日志文件为:%s', logfile)
    
    dsi = DictSourceImport()
    # dsi.readCsv()
    dsi.readMolFile()
    # dsi.readFixCsv()
    logging.info(u'程序执行完成!')
