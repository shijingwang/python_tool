# -*- coding: utf-8 -*-
from tornado.options import define, options
import logging
import os, sys, time
import traceback
import json, base64
import threading
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
        if not os.path.exists(dict_conf.worker_bitmapdir):
            os.makedirs(dict_conf.worker_bitmapdir)
        self.db_dict = ConUtil.connect_mysql(dict_conf.MYSQL_DICT_AGENT)
    
    def read_sdf_task(self):
        sdf_text = self.redis_server.rpop(CK.R_SDF_IMPORT)
        if not sdf_text:
            return
        logging.info(u'从redis接收数据:%s', sdf_text)
        self.db_dict.reconnect()
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
    
    def read_sdf_task_thread(self):
        logging.info(u'启动SDF数据读取线程')
        while True:
            size = self.redis_server.llen(CK.R_SDF_IMPORT)
            logging.info(u'SDF任务队列的大小为:%s', size)
            if size > 0:
                self.read_sdf_task()
            time.sleep(5)
    
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
        return result
    
    def import_dict(self):
        dict_v = self.redis_server.rpop(CK.R_DICT_IMPORT)
        if not dict_v:
            return
        # logging.info(u'接收到的字黄写入数据为:%s', dict_v)
        self.db_dict.reconnect()
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
                    self.redis_server.lpush(CK.R_DICT_SYN, redis_msg)
                    logging.warn(u'mol_id:%s 是用户新增记录,需要将数据同步至离线数据库', mol_id)
            return
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
            size = self.redis_server.llen(CK.R_DICT_IMPORT)
            logging.info(u'Dict队列中的数据大小为:%s', size)
            if size == 0:
                time.sleep(3)
                continue
            self.import_dict()

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
    da1 = DictAgent()
    da2 = DictAgent()
    da1.start_agent1()
    da2.start_agent2()
    '''
    da1.db_dict.execute('truncate table sdf_log;')
    da1.redis_server.lpush(CK.R_SDF_IMPORT, '{"file_key":"143s23sdsre132141343d", "file_path":"/home/kulen/Documents/xili_data/Sample_utf8.sdf"}')
    da1.read_sdf_task()
    da2.import_dict()
    '''
    
    logging.info(u'程序运行完成')
