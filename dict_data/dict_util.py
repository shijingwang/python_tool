# -*- coding: utf-8 -*-
import json
import logging
import os, sys
from tornado.options import define, options
import traceback
import time

try:
    import python_tool
except ImportError:
    fp = os.path.abspath(__file__)
    sys.path.append(fp[0:fp.rfind('python_tool') + 11])
import CK
from common.con_util import ConUtil
from common.cas_util import CasUtil
from dict_compound import DictCompound
import dict_conf


class DictUtil(DictCompound):
    
    def __init__(self):
        self.redis_server = ConUtil.connect_redis(dict_conf.REDIS_SERVER)
        # self.db_dict = ConUtil.connect_mysql(dict_conf.MYSQL_DICT_AGENT)
        self.db_dict_source = ConUtil.connect_mysql(dict_conf.MYSQL_DICT_SOURCE)
        self.cu = CasUtil()
    
    def write_redis_data(self):
        self.redis_server.flushall()
        # self.db_dict.execute('truncate table log_sdf')
        self.redis_server.lpush(CK.R_SDF_IMPORT, '{"file_key":"143s23sdsre132141343d123", "code":"1234", "file_path":"/home/kulen/Documents/xili_data/xili_3_1.sdf"}')
        # self.redis_server.lpush(CK.R_SDF_IMPORT, '{"file_key":"143s23sdsre132141343d123", "code":"1234", "file_path":"/home/kulen/Documents/xili_data/Sample_utf8.sdf"}')
    
    def import_table_data(self):
        sql = 'select * from dic_source_data where has_dispose=0 order by id desc'
        rs = self.db_dict_source.query(sql)
        logging.info(u"导入数据量为:%s", len(rs))
        for r in rs:
            try:
                # logging.info(u'处理id:%s cas_no:%s的记录', r['id'], r['cas_no'])
                if r['cas_no'] and not self.cu.cas_check(r['cas_no']):
                    logging.info(u'CAS号:%s 校验失败', r['cas_no'])
                    continue
                # data_type 1--表示有smile和inchi数据  2--表示有mol数据，无smile inchi数据
                if r['data_type'] == 1:
                    self.extract_inchi_data(r)
                else:
                    self.extract_mol_data(r)
                data_dict = {'name_en':r['name_en'], 'name_en_alias':r['name_en_alias'], 'name_cn':r['name_cn'], 'name_cn_alias':r['name_cn_alias'], 'cas_no':r['cas_no']}
                data_dict['mol'] = r['mol']
                # 确定数据更新方式　1--对数据进行写入 2--对字典数据进行修正
                if r['write_type'] == 1:
                    data_dict['source'] = 'spider'
                    data_dict['wtype'] = 'insert'
                    data_dict['mol_id'] = ''
                else:
                    data_dict['source'] = 'fix'
                    data_dict['wtype'] = 'update'
                    data_dict['mol_id'] = r['mol_id']
                dict_create_json = json.dumps(data_dict)
                # lpush 优先级比较低
                self.redis_server.lpush(CK.R_DICT_CREATE, dict_create_json)
            except Exception, e:
                logging.error(u"处理cas:%s 产品:%s ErrMsg:%s", r['cas_no'], r['name_en'], e)
                logging.error(traceback.format_exc())
            sql = 'update dic_source_data set has_dispose=1 where id=%s'
            self.db_dict_source.execute(sql, r['id'])
            # break
    
    def extract_inchi_data(self, r):
        if r['smiles'] and not r['inchi']:
            c = 'echo "%s" | babel -ismi -oinchi'
            c = c % r['smiles']
            result = os.popen(c).read().replace('\r', '').replace('\n', '').strip()
            if not result:
                raise Exception(555, '无法由InChI生成smile')
            r['inchi'] = result
        if not r['inchi'].startswith('InChI='):
            r['inchi'] = 'InChI=' + r['inchi']
        c = 'echo "%s" | babel -iinchi -ocan'
        c = c % r['inchi']
        result = os.popen(c).read().replace('\r', '').replace('\n', '').strip()
        if not result:
            raise Exception(555, 'InChI格式不正确')
        c = 'echo "%s" | babel -iinchi -omol --gen2d'
        c = c % r['inchi']
        # logging.info(u'执行生成mol命令:%s', c)
        result = os.popen(c).read()
        r['mol'] = result
    
    def extract_mol_data(self, r):
        pass
        
    def redis_test(self):
        self.redis_server.set('user', 'matrix')
        print self.redis_server.get('user')
    
    def string_test(self):
        import re
        p = re.compile(r'>\s*<\w+')
        match = p.match('><synonym> (1717)\n')
        if match:
            print 'Match:' + match.group()
        else:
            print u'未匹配'
    
    def fix_dict_daemon(self):
        while(True):
            time.sleep(3)
            self.import_table_data()
        
if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')
    
    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="/tmp/dict_util.log", help="NSQ topic")
    define("func_name", default="import_table_data")
    options.parse_command_line()
    logfile = options.logfile
    fmt = '%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s'
    handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=100 * 1024 * 1024, backupCount=10)  # 实例化handler
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt)  # 实例化formatter
    handler.setFormatter(formatter)  # 为handler添加formatter
    logging.getLogger('').addHandler(handler)
    logging.info(u'写入的日志文件为:%s', logfile)
    
    du = DictUtil()
    du.fix_dict_daemon()
    # du.import_table_data()
    # du.write_redis_data()
    # du.string_test()
    logging.info(u'完成初始化!');
