# -*- coding: utf-8 -*-
from tornado.options import define, options
import logging
import os, sys
try:
    import python_tool
except ImportError:
    fp = os.path.abspath(__file__)
    sys.path.append(fp[0:fp.rfind('python_tool') + 11])
from common.con_util import ConUtil
import dict_conf

class SdfWorker(object):
    
    def __init__(self):
        self.db_dict = ConUtil.connect_mysql(dict_conf.MYSQL_DICT)
        if not os.path.exists(dict_conf.bitmapdir):
            os.makedirs(dict_conf.bitmapdir)
        self.tmp_mol1 = '/tmp/mol1.mol'
        self.tmp_mol2 = '/tmp/mol2.mol'
        pass

    def delete_file(self, fp):
        try:
            if os.path.exists(fp):
                os.remove(fp)
        except Exception:
            pass

    def import_sdf(self):
        fp = "/home/kulen/PerlProject/python_tool/dict_data/test.sdf"
        fp_reader = open(fp)
        mol = '\n'
        name = ''
        value = ''
        attr_list = []
        while 1:
            line = fp_reader.readline()
            if not line:
                break
            if line.startswith('>  <'):
                if name:
                    value = value.replace('\n', '')
                    # print "Name:%s Value:%s" % (name, value)
                    attr_list.append({'name':name, 'value':value})
                name = ''
                value = ''
                name = line[line.index('<') + 1:line.rindex('>')]
                continue
            if name:
                value += line
            else:
                mol += line
            check_line = line.replace('\n', '')
            
            # print '---------------------'
            # print '[%s]' % line
            if check_line == '$$$$':
                # print attr_list
                # print mol
                # 已经完成对一个化合物数据的提取
                self.write_dic(attr_list, mol)
                break
            
        fp_reader.close()
        # print html_content
        pass
    
    def write_dic(self, attrs, mol):
        self.check_match(mol)
        pass
    
    def check_match(self, mol):
        c = "echo \"%s\" | checkmol -axH -" % mol
        result = os.popen(c).read()
        logging.info(u"check_mol_result:%s", result)
        chkresult = result.split('\n')
        result1 = chkresult[0]
        result2 = chkresult[1]
        result2 = result2.split(';')[0]
     
        if 'invalid' in result1:
            raise Exception('无效的Mol文件')
        result1 = result1[0: len(result1) - 1]
        result1 = result1.replace(';', ' and ').replace(':', '=').replace('n_', 'stat.n_')
        sql = 'select stat.mol_id,struc.struc from search_molstat as stat, search_molstruc as struc where (%s) and (stat.mol_id=struc.mol_id)'
        sql = sql % (result1)
        # print "[%s]" % result1
        # print "[%s]" % result2  
        logging.info(u"执行的sql:%s", sql)
        rs = self.db_dict.query(sql)
        if len(rs) == 0:
            return 1

        self.delete_file(self.tmp_mol1)
        mol1_writer = open(self.tmp_mol1, 'w')
        mol1_writer.write(mol)
        mol1_writer.close()
        for r in rs:
            self.delete_file(self.tmp_mol2)
            mol2_writer = open(self.tmp_mol2, 'w')
            mol2_writer.write(r['struc'])
            mol2_writer.close()
            
            c = "%s -aisxgG %s %s" % (dict_conf.MATCHMOL, self.tmp_mol1, self.tmp_mol2)
            
        pass

if __name__ == '__main__':

    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="/tmp/msds_extract.log", help="NSQ topic")
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
    worker = SdfWorker()
    worker.import_sdf()
    logging.info(u'程序运行完成')
