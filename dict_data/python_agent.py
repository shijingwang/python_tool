# -*- coding: utf-8 -*-
from tornado.options import define, options
import logging
import os, sys
import traceback
import json, base64
try:
    import python_tool
except ImportError:
    fp = os.path.abspath(__file__)
    sys.path.append(fp[0:fp.rfind('python_tool') + 11])

import dict_conf
from common.con_util import ConUtil
import CK
from dict_compound import DictCompound


class PythonAgent(DictCompound):
    
    def __init__(self):
        DictCompound.__init__(self)
        if not os.path.exists(dict_conf.worker_bitmapdir):
            os.makedirs(dict_conf.worker_bitmapdir)
        self.db_dict = ConUtil.connect_mysql(dict_conf.MYSQL_DICT_AGENT)
    
    def read_redis_task(self):
        sdf_text = self.redis_server.rpop(CK.R_SDF_IMPORT)
        if not sdf_text:
            return
        logging.info(u'从redis接收数据:%s', sdf_text)
        sdf_json = json.loads(sdf_text)
        md5 = sdf_json['file_key']
        file_path = sdf_json['file_path']
        sql = 'select * from sdf_log where md5=%s'
        rs = self.db_dict.query(sql, md5)
        if len(rs) > 0:
            logging.warn(u'md5:%s file:%s 的数据已经处理', md5, file_path)
            return
        sql = 'insert into sdf_log (file_path,md5,import_time) values (%s,%s,now())'
        self.db_dict.insert(sql, *[file_path, md5])
        self.read_sdf(md5, file_path)
    
    def read_sdf(self, md5, file_path):
        logging.info(u'处理md5:%s  文件为:%s 的数据', md5, file_path)
        fp_reader = open(file_path)
        mol = ''
        name = ''
        value = ''
        goods_dict = {}
        goods_list = []
        prices = []
        total_count = 0
        counter = 0
        while 1:
            line = fp_reader.readline()
            if not line:
                counter += 1
                if counter >= 20:
                    break
            # print line
            if line.startswith('>  <') or line.startswith('$$$$'):
                if name:
                    value = value.replace('\n', '').replace('\r', '')
                    # print "Name:%s Value:%s" % (name, value)
                    if name in ['spec_1', 'spec_2', 'spec_3', 'spec_4', 'spec_5']:
                        prices.append(value)
                    # 商品价格表数据
                    goods_dict[name] = value
                if line.startswith('>  <'):
                    name = ''
                    value = ''
                    counter = 0
                    name = line[line.index('<') + 1:line.rindex('>')]
                    continue
            if name:
                value += line
            else:
                mol += line
            check_line = line.replace('\n', '').replace('\r', '')
            
            # 表示有多个一个产品结束
            if check_line == '$$$$':
                total_count += 1
                try:
                    v_d = {}
                    for key in dict_conf.SDF_KEY:
                        # goods_key gk
                        for gk in goods_dict.keys():
                            if gk in dict_conf.SDF_KEY[key]:
                                v_d[key] = goods_dict[gk]
                    result = self.write_dic(v_d, mol)
                    if result['mol_id'] > 0:
                        for price in prices:
                            goods = {
                                'mol_id':result['mol_id'],
                                'cas_no':v_d['cas_no'],
                                'purity':goods_dict.get('PURITY', ''),
                                'lead_time':goods_dict.get('LEAD_TIME', ''),
                                'stock':goods_dict.get('STOCK', ''),
                                'capacity':goods_dict.get('CAPACITY', ''),
                                'price':price
                            }
                            goods_list.append(goods)
                except Exception, e:
                    logging.error(u"处理产品时出错:%s", goods_dict)
                    logging.error(traceback.format_exc())
                mol = ''
                name = ''
                value = ''
                counter = 0
                goods_dict = {}
                prices = []
        fp_reader.close()
        # SDF处理结果
        p_result = {}
        p_result['file_key'] = md5
        p_result['code'] = 0
        p_result['msg'] = 'success'
        p_result['total_count'] = total_count
        p_result['new_dict_count'] = 0
        p_result['prices'] = goods_list
        j_result = json.dumps(p_result)
        logging.info(u'生成的JSON数据为:%s', j_result)
        self.redis_server.lpush(CK.R_SDF_EXPORT, j_result)
    
    def write_dic(self, data_dict, mol):
        result = {'code':0, 'msg':'success', 'mol_id':-1}
        logging.info(u"处理属性:%s数据", data_dict)
        if not self.cu.cas_check(data_dict['cas_no']):
            result['code'] = -1;result['msg'] = u'相应的cas号数据错误'
            return result
        self.fu.delete_file(self.tmp_mol1)
        mol1_writer = open(self.tmp_mol1, 'w')
        mol1_writer.write(mol)
        mol1_writer.close()
        check_mol_id = self.check_match(data_dict['cas_no'], mol)
        if check_mol_id > 0:
            result['mol_id'] = check_mol_id
            return result
        
        if not data_dict.get('name_en'):
            data_dict['name_en'] = ''
        if not data_dict.get('name_en_alias'):
            data_dict['name_en_alias'] = ''
        if not data_dict.get('name_cn'):
            data_dict['name_cn'] = ''
        if not data_dict.get('name_cn_alias'):
            data_dict['name_cn_alias'] = ''
        data_dict['mol'] = mol
        dict_create_json = json.dumps(data_dict)
        self.redis_server.lpush(CK.R_DICT_CREATE, dict_create_json)
        result['mol_id'] = -1
        # TODO
        return result
    
    def import_dict(self):
        dict_v = self.redis_server.rpop(CK.R_DICT_IMPORT)
        if not dict_v:
            return
        # logging.info(u'接收到的字黄写入数据为:%s', dict_v)
        dict_j = json.loads(dict_v)
        mol_id = dict_j['mol_id']
        sql = 'select * from search_moldata where mol_id=%s'
        rs = self.db_dict.query(sql, mol_id)
        if len(rs) > 0 and dict_j['moldata']['type'] == 'insert':
            logging.warn(u'处理数据时，数据库中已经有相应的记录:%s', dict_v)
            return
        logging.info(u'写入mol_id:%s  操作类型:%s 的数据', mol_id, dict_j['moldata']['type'])
        self.db_dict.execute(dict_j['moldata']['sql'], *dict_j['moldata']['params'])
        self.db_dict.execute(dict_j['molstruc']['sql'], *dict_j['molstruc']['params'])
        self.db_dict.execute(dict_j['molstat']['sql'], *dict_j['molstat']['params'])
        self.db_dict.execute(dict_j['molfgb']['sql'], *dict_j['molfgb']['params'])
        self.db_dict.execute(dict_j['molcfp']['sql'], *dict_j['molcfp']['params'])
        self.db_dict.execute(dict_j['pic2d']['sql'], *dict_j['pic2d']['params'])
        # 写入图片数据
        img_writer = open(dict_conf.agent_bitmapdir + '/' + dict_j['mol_pic_path'], 'wb')
        img_writer.write(base64.decodestring(dict_j['mol_pic']))
        img_writer.close()

if __name__ == '__main__':

    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="/tmp/sdf_import.log", help="NSQ topic")
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
    pa = PythonAgent()
    '''
    pa.db_dict.execute('truncate table sdf_log;')
    pa.redis_server.lpush(CK.R_SDF_IMPORT, '{"file_key":"143s23sdsre132141343d", "file_path":"/home/kulen/Documents/xili_data/xili_3.sdf"}')
    pa.read_redis_task()
    '''
    pa.import_dict()
    logging.info(u'程序运行完成')
