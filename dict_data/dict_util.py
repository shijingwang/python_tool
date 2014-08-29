# -*- coding: utf-8 -*-
import json, base64
import logging
import os, sys, time
import threading
from tornado.options import define, options
import traceback

try:
    import python_tool
except ImportError:
    fp = os.path.abspath(__file__)
    sys.path.append(fp[0:fp.rfind('python_tool') + 11])
import CK
from common.con_util import ConUtil
from dict_compound import DictCompound
import dict_conf


class DictUtil(DictCompound):
    
    def __init__(self):
        self.redis_server = ConUtil.connect_redis(dict_conf.REDIS_SERVER)
        self.db_dict = ConUtil.connect_mysql(dict_conf.MYSQL_DICT_AGENT)
        self.db_dict_source = ConUtil.connect_mysql(dict_conf.MYSQL_DICT_SOURCE)
    
    def write_redis_data(self):
        self.redis_server.flushall()
        self.db_dict.execute('truncate table sdf_log')
        # self.redis_server.lpush(CK.R_SDF_IMPORT, '{"file_key":"143s23sdsre132141343d123", "file_path":"/home/kulen/Documents/xili_data/xili_3_1.sdf"}')
        self.redis_server.lpush(CK.R_SDF_IMPORT, '{"file_key":"143s23sdsre132141343d123", "file_path":"/home/kulen/Documents/xili_data/Sample_utf8.sdf"}')
    
    def import_table_data(self):
        sql = 'select * from dic_source_data'
        rs = self.db_dict_source.query(sql)
        for r in rs:
            try:
                # logging.info(u'处理id:%s cas_no:%s的记录', r['id'], r['cas_no'])
                if not r['cas_no']:
                    logging.info(u'id:%s 记录无cas号', r['id'])
                    continue
                if not self.cu.cas_check(r['cas_no']):
                    logging.info(u'CAS号:%s 校验失败', r['cas_no'])
                    continue
                if not r['inchi'].startswith('InChI='):
                    r['inchi'] = 'InChI=' + r['inchi']
                c = 'echo "%s" | babel -iinchi -ocan'
                c = c % r['inchi']
                result = os.popen(c).read().replace('\r', '').replace('\n', '').strip()
                if not result:
                    logging.info(u"CAS号:%s InChI:%s 格式错误", r['cas_no'], r['inchi'])
                    continue
                data_dict = {'name_en':r['name_en'], 'name_en_alias':r['name_en_alias'], 'name_cn':r['name_cn'], 'name_cn_alias':r['name_cn_alias'], 'cas_no':r['cas_no']}
                c = 'echo "%s" | babel -iinchi -omol --gen2d'
                c = c % r['inchi']
                # logging.info(u'执行生成mol命令:%s', c)
                result = os.popen(c).read()
                data_dict['mol'] = result
                dict_create_json = json.dumps(data_dict)
                # lpush 优先级比较低
                self.redis_server.lpush(CK.R_DICT_CREATE, dict_create_json)
            except Exception, e:
                logging.error(u"处理cas:%s 产品:%s ErrMsg:%s", r['cas_no'], r['name_en'], e)
                logging.error(traceback.format_exc())
                
            # break
        
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
        
if __name__ == '__main__':
    du = DictUtil()
    # du.write_redis_data()
    # du.redis_test()
    du.string_test()
    print u'完成初始化!'
