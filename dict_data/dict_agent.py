# -*- coding: utf-8 -*-
from tornado.options import define, options
import logging
import os, sys, time
import traceback
import json, base64
import threading
import re
try:
    import python_tool
except ImportError:
    fp = os.path.abspath(__file__)
    sys.path.append(fp[0:fp.rfind('python_tool') + 11])
import dict_conf
from common.con_util import ConUtil
import CK
from dict_compound import DictCompound


class DictAgent(DictCompound):
    
    def __init__(self):
        DictCompound.__init__(self)
        self.n_p = re.compile(r'>\s*<\w+')
        self.db_dict = ConUtil.connect_mysql(dict_conf.MYSQL_DICT_AGENT)
    
    def read_sdf_task(self):
        sdf_text = self.redis_server.rpop(CK.R_SDF_IMPORT)
        if not sdf_text:
            return
        logging.info(u'从redis接收数据:%s', sdf_text)
        sdf_json = json.loads(sdf_text)
        md5 = sdf_json['file_key']
        file_path = sdf_json['file_path']
        code = sdf_json['code']
        sql = 'select * from log_sdf where md5=%s'
        rs = self.db_dict.query(sql, md5)
        if len(rs) > 0:
            logging.warn(u'md5:%s file:%s 的数据已经处理', md5, file_path)
        sql = 'insert into log_sdf (file_path,md5,import_time) values (%s,%s,now())'
        self.db_dict.insert(sql, *[file_path, md5])
        self.read_sdf(code, md5, file_path)
    
    def read_sdf_task_thread(self):
        logging.info(u'启动SDF数据读取线程')
        while True:
            try:
                size = self.redis_server.llen(CK.R_SDF_IMPORT)
                logging.info(u'SDF任务队列的大小为:%s', size)
                if size == 0:
                    time.sleep(3)
                    continue
                self.read_sdf_task()
            except Exception, e:
                time.sleep(3)
                logging.error(u"SDF任务队列数据处理出错:%s", e)
                logging.error(traceback.format_exc())
    
    def read_sdf(self, code, md5, file_path):
        logging.info(u'处理md5:%s  文件为:%s 的数据', md5, file_path)
        
        sql = 'select * from sdf_mapping'
        rs = self.db_dict.query(sql)
        name_mapping = {}
        for r in rs:
            alias_names = []
            alias_names.append(r['standard_name'].lower())
            if not r['alias_name']:
                r['alias_name'] = ''
            r['alias_name'] = r['alias_name'].strip()
            if len(r['alias_name']) > 0:
                _names = r['alias_name'].split('|')
                for name in _names:
                    if len(name.strip()) == 0:
                        continue
                    alias_names.append(name.strip().lower())
            name_mapping[r['standard_name'].lower()] = alias_names
        logging.info(u'处理SDF字段映射名是:%s', name_mapping)
        fp_reader = open(file_path)
        mol = ''
        name = ''
        value = ''
        goods_dict = {}
        goods_list = []
        prices = []
        total_count = 0
        price_total_count = 0
        # 连续出现多个空行，自动关闭文件
        counter = 0
        while 1:
            line = fp_reader.readline()
            if not line:
                counter += 1
                if counter >= 20:
                    break
            # print line
            check_line = line.replace('\n', '').replace('\r', '').strip()
            if self.n_p.match(check_line) or check_line.startswith('$$$$'):
                if name:
                    value = value.replace('\n', '').replace('\r', '').strip()
                    # print "Name:%s Value:%s" % (name, value)
                    goods_dict[name.lower()] = value
                if self.n_p.match(check_line):
                    name = ''
                    value = ''
                    counter = 0
                    name = line[line.index('<') + 1:line.rindex('>')].strip()
                    continue
            if name:
                value += line
            else:
                mol += line
            
            # 表示有多个一个产品结束
            if check_line == '$$$$':
                total_count += 1
                try:
                    # 完成字段数据的映射关系
                    v_d = {}
                    # name_key nk
                    for nk in name_mapping:
                        # goods_key gk
                        for gk in goods_dict.keys():
                            if gk in name_mapping[nk]:
                                v_d[nk] = goods_dict[gk]
                    # 内部逻辑处理使用的逻辑
                    v_d['name_en'] = v_d.get('product_name', '')
                    v_d['name_cn'] = v_d.get('product_name_cn', '')
                    v_d['cas_no'] = v_d.get('cas_number', '')
                    result = self.write_dic(v_d, mol)
                    goods_list = []
                    err_msg = ''
                    if result['mol_id'] > 0:
                        prices = []
                        # price_key pk
                        for pk in v_d:
                            if pk in ['spec_1', 'spec_2', 'spec_3', 'spec_4', 'spec_5']:
                                prices.append(v_d.get(pk))
                        for price in prices:
                            if not price:
                                err_msg += u'CAS号:%s 存在无价格的规格  ' % (v_d.get('cas_no', ''))
                                continue
                            goods = {
                                'mol_id':result['mol_id'],
                                'cas_no':v_d.get('cas_number', ''),
                                'product_name': v_d.get('name_en', ''),
                                'product_name_cn': v_d.get('name_cn', ''),
                                'purity':v_d.get('purity', ''),
                                'lead_time':v_d.get('lead_time', ''),
                                'stock':v_d.get('stock', ''),
                                'capacity':v_d.get('capacity', ''),
                                'price':price,
                                'sku':v_d.get('sku', '')
                            }
                            goods_list.append(goods)
                            price_total_count += 1
                        if len(goods_list) == 0:
                            err_msg = u'CAS号:%s %s' % (v_d.get('cas_no', ''), u'未指定价格')
                        if len(err_msg) == 0:
                            err_msg = 'success'
                    else:
                        err_msg = u'CAS号:%s %s' % (v_d.get('cas_no', ''), result['msg'])
                    if not err_msg == 'success':
                        logging.info(err_msg)
                    p_result = {}
                    p_result['file_key'] = md5
                    p_result['code'] = code
                    p_result['msg'] = err_msg
                    p_result['finish'] = 0
                    p_result['total_count'] = 1
                    p_result['new_dict_count'] = 0
                    p_result['prices'] = goods_list
                    j_result = json.dumps(p_result)
                    # logging.info(u'生成的JSON数据为:%s', j_result)
                    self.redis_server.lpush(CK.R_SDF_EXPORT, j_result)
                except Exception, e:
                    logging.error(u"处理产品时出错:%s", goods_dict)
                    logging.error(traceback.format_exc())
                mol = ''
                name = ''
                value = ''
                counter = 0
                goods_dict = {}
        fp_reader.close()
        # 推送一条处理完的结果
        p_result = {}
        p_result['file_key'] = md5
        p_result['code'] = code
        p_result['msg'] = 'success'
        p_result['finish'] = 1
        p_result['total_count'] = 1
        p_result['new_dict_count'] = 0
        p_result['prices'] = []
        j_result = json.dumps(p_result)
        self.redis_server.lpush(CK.R_SDF_EXPORT, j_result)
        # SDF处理结果
        logging.info(u'file key:%s 化合物总数:%s 生成价格数据条数:%s', md5, total_count, price_total_count)
    
    def write_dic(self, data_dict, mol):
        result = {'code':0, 'msg':'success', 'mol_id':-1}
        logging.info(u"处理属性:%s数据", data_dict)
        # 允许cas号为空的数据
        if data_dict['cas_no'] and not self.cu.cas_check(data_dict['cas_no']):
            result['code'] = -1;result['msg'] = u'CAS号不符合规则'
            return result
        self.fu.delete_file(self.tmp_mol1)
        mol1_writer = open(self.tmp_mol1, 'w')
        mol1_writer.write(mol)
        mol1_writer.close()
        try:
            check_mol_id = self.check_match(data_dict['cas_no'], mol)
        except Exception, e:
            result['code'] = -1; result['msg'] = str(e)
            return result
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
        data_dict['source'] = 'user'
        data_dict['wtype'] = 'insert'
        dict_create_json = json.dumps(data_dict)
        # 推送任务, rpush优先级比较高,rpop
        self.redis_server.rpush(CK.R_DICT_CREATE, dict_create_json)
        
        counter = 0
        result['mol_id'] = -1
        while True:
            counter += 1 
            # 检查数据是否写完
            sql = 'select * from search_moldata where cas_no=%s'
            rs = self.db_dict.query(sql, data_dict['cas_no'])
            for r in rs:
                result['mol_id'] = r['mol_id']
                break
            if counter >= 6:
                break
            time.sleep(2)
        if result['mol_id'] < 0:
            result['msg'] = u'数据写入字典失败'
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
        if len(rs) > 0 and dict_j['search_moldata']['type'] == 'insert':
            for r in rs:
                if r['cas_no'] == dict_j['cas_no'] and r['formula'] == dict_j['formula']:
                    logging.warn(u'mol_id:%s 记录已经在数据库中存在', mol_id)                    
                    pass
                else:
                    # 将用户新增数据同步出去
                    redis_msg = {} 
                    self.read_sql(redis_msg, r['mol_id'])
                    redis_msg = json.dumps(redis_msg)
                    self.redis_server.lpush(CK.R_DICT_SYN, redis_msg)
                    logging.warn(u'mol_id:%s 是用户新增记录,需要将数据同步至离线数据库', mol_id)
            return
        if dict_j['search_moldata']['type'] == 'insert':
            check_mol_id = self.check_match(dict_j['cas_no'], dict_j['mol'])
            if check_mol_id > 0:
                logging.error(u'同步mol_id:%s 数据时, 与当前数据库中的mol_id:%s 数据表示同一化合物.', dict_j['mol_id'], check_mol_id)
                return
        
        # 对字典的数据进行更新, 首先删除其余5张表无用的数据
        if len(rs) > 0 and dict_j['search_moldata']['type'] == 'update':
            self.delete_data(mol_id)
        
        # 执行SQL写入与更新操作
        self.write_json_data(mol_id, dict_j)
        # 写入图片数据
        pic_fp = dict_conf.agent_bitmapdir + '/' + dict_j['mol_pic_path']
        if not os.path.exists(pic_fp[0:pic_fp.rfind('/')]):
            os.makedirs(pic_fp[0:pic_fp.rfind('/')])
        img_writer = open(pic_fp, 'wb')
        img_writer.write(base64.decodestring(dict_j['mol_pic']))
        img_writer.close()
    
    def import_dict_thread(self):
        logging.info(u'启动字典数据写入线程')
        while True:
            try:
                size = self.redis_server.llen(CK.R_DICT_IMPORT)
                logging.info(u'ImportDict队列中的数据大小为:%s', size)
                if size == 0:
                    time.sleep(3)
                    continue
                self.import_dict()
            except Exception, e:
                time.sleep(3)
                logging.error(u"ImportDict队列中的数据处理出错:%s", e)
                logging.error(traceback.format_exc())

    def start_agent1(self):
        dict_thread = self.__getattribute__('import_dict_thread')
        t1 = threading.Thread(target=dict_thread)
        t1.start()
    
    def start_agent2(self):
        sdf_thread = self.__getattribute__('read_sdf_task_thread')
        t2 = threading.Thread(target=sdf_thread)
        t2.start()
        
if __name__ == '__main__':

    reload(sys)
    sys.setdefaultencoding('utf-8')
    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="/tmp/dict_agent.log", help="NSQ topic")
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
    da1 = DictAgent()
    
    time.sleep(2)
    da2 = DictAgent()
    time.sleep(2)
    da1.start_agent1()
    time.sleep(2)
    da2.start_agent2()
    time.sleep(2)
    '''
    da1.redis_server.flushall()
    da1.db_dict.execute('truncate table log_sdf;')
    da1.redis_server.lpush(CK.R_SDF_IMPORT, '{"file_key":"143s23sdsre132141343d", "code":"123456", "file_path":"/home/kulen/Documents/xili_data/RMB_test.sdf"}')
    # da1.redis_server.lpush(CK.R_SDF_IMPORT, '{"file_key":"143s23sdsre132141343d", "code":"123456", "file_path":"/home/kulen/Documents/xili_data/xili_3_1.sdf"}')
    da1.read_sdf_task()
    '''
    logging.info(u'程序运行完成')
