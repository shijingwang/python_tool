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
from common.con_util import ConUtil
from dict_compound import DictCompound
import dict_conf
import CK

class DictWorkerV2(DictCompound):
    
    def __init__(self):
        DictCompound.__init__(self)
        if not os.path.exists(dict_conf.worker_bitmapdir):
            os.makedirs(dict_conf.worker_bitmapdir)
        self.db_dict = ConUtil.connect_mysql(dict_conf.MYSQL_DICT_WORKER)
        self.db_dict_source = ConUtil.connect_mysql(dict_conf.MYSQL_DICT_SOURCE)
        sql = 'select fpdef from moldb_fpdef'
        rs = self.db_dict.query(sql)
        for r in rs:
            self.fpdef = r['fpdef']
    
    def handler(self, signum, frame):
        raise Exception(u"Process Timeout")

    def read_dict_task(self):
        dict_v = self.redis_server.rpop(CK.R_DICT_CREATE)
        if not dict_v:
            return
        # logging.info(u'接收到的任务为:%s', dict_v)
        dict_j = json.loads(dict_v)
        
        # 写入mol文件数据，以便进行后续操作
        self.fu.delete_file(self.tmp_mol1)
        mol1_writer = open(self.tmp_mol1, 'w')
        mol1_writer.write(dict_j['mol'])
        mol1_writer.close()
        if dict_j['wtype'] == 'insert':
            self.write_dic(dict_j)
        if dict_j['wtype'] == 'update':
            self.update_dic(dict_j)

    def read_dict_task_thread(self):
        logging.info(u'启动字典创建同步线程')
        while True:
            try:
                size = self.redis_server.llen(CK.R_DICT_CREATE)
                logging.info(u'DictCreate队列中的数据大小为:%s', size)
                if size == 0:
                    time.sleep(1)
                    continue
                self.read_dict_task()
            except Exception, e:
                time.sleep(1)
                logging.error(u"DictCreate队列中的数据处理出错,%s", e)
                logging.error(traceback.format_exc())
    
    # wtype 写入类型, insert 写入新数据 update 更新数据
    def write_dic_common(self, mol_id, wtype, data_dict):
        # 填充没有字典的数据
        if not data_dict.get('name_en'):
            data_dict['name_en'] = ''
        if not data_dict.get('name_en_alias'):
            data_dict['name_en_alias'] = ''
        if not data_dict.get('name_cn'):
            data_dict['name_cn'] = ''
        if not data_dict.get('name_cn_alias'):
            data_dict['name_cn_alias'] = ''
        params = [mol_id]
        params.append(data_dict['name_en'])
        params.append(data_dict['name_en_alias'])
        params.append(data_dict['name_cn_alias'])
        params.append(data_dict['name_cn'])
        params.append(data_dict['cas_no'])
        c = "obprop %s 2>/dev/null | awk -F\"\\t\" '{print $1}' | cut -c 17- | head -16 | tail -15"
        c = c % (self.tmp_mol1)
        result = os.popen(c).read()
        results = result.split('\n');
        for i in range(0, 15):
            v = results[i].strip()
            if not v:
                continue
            params.append(v)
            # print "%s : %s" % ((i + 1), v)
        if wtype == 'insert':
            # 用户添加的数据，则需要进行相应的审核
            if data_dict.get('source') == 'user':
                params.append(1)
                params.append(0)
            else:
                params.append(0)
                params.append(1)
            sql = '''INSERT INTO search_moldata (mol_id, mol_name, en_synonyms, zh_synonyms, name_cn, cas_no, 
                                                    formula,mol_weight,exact_mass,smiles,inchi,
                                                    num_atoms,num_bonds,num_residues,sequence,
                                                    num_rings,logp,psa,mr,goods_count,is_user_add,is_audit) VALUES (
                                                    %s,%s,%s,%s,%s,%s,
                                                    %s,%s,%s,%s,%s,
                                                    %s,%s,%s,%s,
                                                    %s,%s,%s,%s,0,%s,%s
                                                    )'''
            logging.info(u"写入新数据,mol_id:%s!", mol_id)
            # logging.info(sql)
            self.db_dict.insert(sql, *params)
            
            sql = 'update dic_source_data set mol_id=%s where id=%s'
            self.db_dict_source.execute(sql, mol_id, data_dict['source_id'])
        
        # 防止数据更新, 带来的冲击与影响
        elif wtype == 'update':
            sql = '''update search_moldata set formula=%s,mol_weight=%s,exact_mass=%s,smiles=%s,inchi=%s,
                                                    num_atoms=%s,num_bonds=%s,num_residues=%s,sequence=%s,
                                                    num_rings=%s,logp=%s,psa=%s,mr=%s
                                                    where mol_id=%s
                                                    '''
            logging.info(u"更新数据,mol_id:%s!", mol_id)
            u_params = params[6:]
            u_params.append(mol_id)
            self.db_dict.execute(sql, *u_params)
            update_mol_sql = sql
            update_mol_params = u_params
        else:
            raise Exception(555, '不支持的业务类型')
        
        # 删除其余5张表无用的数据
        self.delete_data(mol_id)

        # 对mol文件进行相应的格式化, 并对数据进行保存
        c = "echo \"%s\" | %s -ms 2>&1" % (data_dict['mol'], dict_conf.CHECKMOL_V2)
        result = os.popen(c).read()
        data_dict['mol'] = result
        # print "molformat>>>" + result
        sql = "insert into search_molstruc values (%s,%s,0,0)"
        params = [mol_id, data_dict['mol']]
        self.db_dict.insert(sql, *params)
        
        # 写入加速表数据
        self.update_stat_table(mol_id, data_dict['mol'])
        
        # 生成图片数据
        pic_dir = self.generate_pic_path(mol_id)
        pic_fp = dict_conf.worker_bitmapdir + '/' + pic_dir
        if not os.path.exists(pic_fp[0:pic_fp.rfind('/')]):
            os.makedirs(pic_fp[0:pic_fp.rfind('/')])
        self.fu.delete_file(pic_fp)
        # print pic_fp
        # print pic_dir
        c = "echo \"%s\" | %s %s - 2>&1"
        c = c % (data_dict['mol'], dict_conf.MOL2PS, dict_conf.mol2psopt)
        molps = os.popen(c).read()
        c = "echo \"%s\" | %s -q -sDEVICE=bbox -dNOPAUSE -dBATCH  -r300 -g500000x500000 - 2>&1"
        c = c % (molps, dict_conf.GHOSTSCRIPT)
        bb = os.popen(c).read()
        bbs = bb.split('\n')
        bblores = bbs[0].replace('%%BoundingBox:', '').lstrip()
        bbcorner = bblores.split(' ')
        if len(bbcorner) >= 4:
            bbleft = int(bbcorner[0])
            bbbottom = int(bbcorner[1])
            bbright = int(bbcorner[2])
            bbtop = int(bbcorner[3])
            xtotal = (bbright + bbleft) * dict_conf.scalingfactor
            ytotal = (bbtop + bbbottom) * dict_conf.scalingfactor
        if xtotal > 0 and ytotal > 0:
            molps = '%s %s scale\n%s' % (dict_conf.scalingfactor, dict_conf.scalingfactor, molps)
        else:
            xtotal = 99; ytotal = 55
            molps = '''%!PS-Adobe
                    /Helvetica findfont 14 scalefont setfont
                    10 30 moveto
                    (2D structure) show
                    10 15 moveto
                    (not available) show
                    showpage\n''';
        gsopt1 = " -r300 -dGraphicsAlphaBits=4 -dTextAlphaBits=4 -dDEVICEWIDTHPOINTS=%s -dDEVICEHEIGHTPOINTS=%s -sOutputFile=%s"
        gsopt1 = gsopt1 % (xtotal, ytotal, pic_fp)
        c = "echo \"%s\" | %s -q -sDEVICE=pnggray -dNOPAUSE -dBATCH %s - "
        c = c % (molps, dict_conf.GHOSTSCRIPT, gsopt1)
        # print 'command>>' + c
        result = os.popen(c).read()
        # print 'pic_result>>' + result
        c = "file \"%s\" | awk '{print $5, $7}' | awk -F\",\" '{print $1}'"
        c = c % pic_fp
        result = os.popen(c).read().replace('\n', '')
        pic_width = result.split(' ')[0]
        pic_height = result.split(' ')[1]
        # print 'pic_size>>' + result
        sql = "insert into search_pic2d (mol_id,type,status,s_pic,s_width,s_height) values (%s,1,1,%s,%s,%s)"
        params = [mol_id, pic_dir, pic_width, pic_height]
        # print sql
        self.db_dict.insert(sql, *params);
        
        # 将数据推送到服务器端
        # 提取mol文件，以便进行匹配
        logging.info(u'将mol_id:%s 字典数据同步至服务端', mol_id)
        redis_msg = {}
        self.read_sql(redis_msg, mol_id)
        self.read_img(redis_msg, mol_id)
        # 在些对redis_msg中的 search_mol是否更新进行处理
        if wtype == 'update':
            redis_msg['search_moldata'] = {'sql':update_mol_sql, 'params':update_mol_params}
            redis_msg['search_moldata']['type'] = 'update'
        redis_msg = json.dumps(redis_msg)
        # print redis_msg
        self.redis_server.lpush(CK.R_DICT_IMPORT, redis_msg)
        return mol_id
    
    # 更新字典数据
    def update_dic(self, data_dict):
        self.write_dic_common(data_dict['mol_id'], 'update', data_dict)
        pass;
    
    def write_dic(self, data_dict):
        logging.info(u"处理cas_no:%s Attr:%s", data_dict['cas_no'], data_dict)
        if data_dict['cas_no'] and not self.cu.cas_check(data_dict['cas_no']):
            logging.warn(u'CAS号校验未通过!')
            return

        check_mol_id = self.check_match(data_dict['cas_no'], data_dict['mol'])
        logging.info('mol_id 已经check完成')
        # 字典中有相应的数据
        if check_mol_id > 0:
            logging.warn(u'数据已经存在!')
            return
        mol_id = self.get_write_molid()
        self.write_dic_common(mol_id, 'insert', data_dict)
    
    def read_img(self, redis_msg, mol_id):
        sql = 'select * from search_pic2d where mol_id=%s'
        rs = self.db_dict.query(sql, mol_id)
        pic_fp = ''
        for r in rs:
            pic_fp = dict_conf.worker_bitmapdir + '/' + r['s_pic']
            redis_msg['mol_pic_path'] = r['s_pic']
        img_reader = open(pic_fp, 'rb')
        v_img = img_reader.read()
        img_reader.close()
        e_img = base64.encodestring(v_img)
        redis_msg['mol_pic'] = e_img
    
    def syn_dict(self):
        dict_v = self.redis_server.rpop(CK.R_DICT_SYN)
        if not dict_v:
            return
        logging.info(u'返回的数据为:%s', dict_v)
        dict_j = json.loads(dict_v)
        mol_id = dict_j['mol_id']
        # 本地数据的新mol_id
        new_mol_id = self.get_write_molid()
        logging.info(u'线上字典数据库同步数据mol_id:%s, 本地字典的mol_id变为:%s', mol_id, new_mol_id)
        # 更改mol_id数据, 原mol_id换id
        sql = 'update search_moldata set mol_id=%s where mol_id=%s'
        self.db_dict.execute(sql, new_mol_id, mol_id)
        sql = 'update search_molstruc set mol_id=%s where mol_id=%s'
        self.db_dict.execute(sql, new_mol_id, mol_id)
        sql = 'update search_pic2d set mol_id=%s where mol_id=%s'
        self.db_dict.execute(sql, new_mol_id, mol_id)
        sql = 'update search_molstat set mol_id=%s where mol_id=%s'
        self.db_dict.execute(sql, new_mol_id, mol_id)
        sql = 'update search_molfgb set mol_id=%s where mol_id=%s'
        self.db_dict.execute(sql, new_mol_id, mol_id)
        sql = 'update search_molcfp set mol_id=%s where mol_id=%s'
        self.db_dict.execute(sql, new_mol_id, mol_id)
        
        # 修改图片路径
        new_pic_dir = self.generate_pic_path(new_mol_id)
        new_pic_fp = dict_conf.worker_bitmapdir + '/' + new_pic_dir
        old_pic_dir = ''
        sql = 'select * from search_pic2d where mol_id=%s'
        rs = self.db_dict.query(sql, new_mol_id)
        for r in rs:
            old_pic_dir = r['s_pic']
        old_pic_fp = dict_conf.worker_bitmapdir + '/' + old_pic_dir
        self.fu.copy_file(old_pic_fp, new_pic_fp)
        
        check_mol_id = self.check_match(dict_j['cas_no'], dict_j['mol'])
        # 没有数据的话，再进行写入操作
        if check_mol_id < 0:
            # 删除mol_data的数据，以便将在线用户写入的字典数据同步过来
            sql = 'delete from search_moldata where mol_id=%s'
            self.db_dict.execute(sql, mol_id)
            self.delete_data(mol_id)
            # 写入服务端的数据
            self.write_json_data(mol_id, dict_j)
        
        # 将本地更改mol_id的数据同步至服务端
        redis_msg = {}
        self.read_sql(redis_msg, new_mol_id)
        self.read_img(redis_msg, new_mol_id)
        redis_msg = json.dumps(redis_msg)
        self.redis_server.lpush(CK.R_DICT_IMPORT, redis_msg)
        
    def generate_pic_path(self, mol_id):
        pic_path = str(mol_id)
        while len(pic_path) < 8:
            pic_path = '0' + pic_path
        pic_dir = pic_path[0:4]
        pic_dir = '%s/%s/%s.png' % (pic_dir[0:2], pic_dir[2:4], mol_id)
        return pic_dir
    
    def syn_dict_thread(self):
        logging.info(u'启动字典数据同步线程')
        while True:
            try:
                size = self.redis_server.llen(CK.R_DICT_SYN)
                logging.info(u'DictSyn队列中的数据大小为:%s', size)
                if size == 0:
                    time.sleep(3)
                    continue
                self.syn_dict()
            except Exception, e:
                time.sleep(3)
                logging.error(u"DictSyn队列中的数据处理出错:%s", e)
                logging.error(traceback.format_exc())

    def start_worker1(self):
        sync_dict_thread = self.__getattribute__('syn_dict_thread')
        t1 = threading.Thread(target=sync_dict_thread)
        t1.start()
    
    def start_worker2(self):
        dict_task_thread = self.__getattribute__('read_dict_task_thread')
        t2 = threading.Thread(target=dict_task_thread)
        t2.start()

if __name__ == '__main__':

    reload(sys)
    sys.setdefaultencoding('utf-8')
    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="/tmp/dict_worker.log", help="NSQ topic")
    define("func_name", default="import_table_data")
    define("sdf_file", default="/home/kulen/Documents/xili_data/xili_2.sdf")
    define("mol_id", default="-1")
    options.parse_command_line()
    logfile = options.logfile
    sdf_file = options.sdf_file
    func_name = options.func_name
    mol_id = int(options.mol_id)
    fmt = '%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s'
    handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=100 * 1024 * 1024, backupCount=10)  # 实例化handler
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt)  # 实例化formatter
    handler.setFormatter(formatter)  # 为handler添加formatter
    logging.getLogger('').addHandler(handler)
    logging.info(u'写入的日志文件为:%s', logfile)
    worker1 = DictWorkerV2()
    time.sleep(2)
    worker2 = DictWorkerV2()
    time.sleep(2)
    worker1.start_worker1()
    time.sleep(2)
    worker2.start_worker2()
    time.sleep(2)
    '''
    worker1.read_dict_task()
    
    '''
    logging.info(u'程序运行完成')
