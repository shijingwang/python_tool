# -*- coding: utf-8 -*-
from tornado.options import define, options
import logging
import os, sys
import csv
import signal
import traceback
import time
try:
    import python_tool
except ImportError:
    fp = os.path.abspath(__file__)
    sys.path.append(fp[0:fp.rfind('python_tool') + 11])
from common.con_util import ConUtil
from common.cas_util import CasUtil
import dict_conf

class DictWorker(object):
    
    def __init__(self):
        self.db_dict = ConUtil.connect_mysql(dict_conf.MYSQL_DICT_WORKER)
        self.db_dict_source = ConUtil.connect_mysql(dict_conf.MYSQL_DICT_SOURCE)
        if not os.path.exists(dict_conf.worker_bitmapdir):
            os.makedirs(dict_conf.worker_bitmapdir)
        self.tmp_mol1 = '/tmp/mol1.mol';self.tmp_mol2 = '/tmp/mol2.mol'
        self.i_mol_id = self.get_start_molid()
        self.cu = CasUtil()
        signal.signal(signal.SIGALRM, self.__getattribute__("handler"))
        sql = 'select fpdef from moldb_fpdef'
        rs = self.db_dict.query(sql)
        for r in rs:
            self.fpdef = r['fpdef']
        # print self.fpdef
        pass

    def delete_file(self, fp):
        try:
            if os.path.exists(fp):
                os.remove(fp)
        except Exception:
            pass
    
    def handler(self, signum, frame):
        raise Exception(u"Process Timeout")
        
    def import_sdf(self, sdf_file):
        fp = sdf_file
        logging.info("导入sdf文档数据:%s", fp)
        fp_reader = open(fp)
        mol = ''
        name = ''
        value = ''
        attr_list = []
        export_list = []
        goods_list = []
        goods_dict = {}
        prices = []
        counter = 0
        while 1:
            line = fp_reader.readline()
            if not line:
                counter += 1
                if counter >= 20:
                    break
            # print '=======' + line
            if line.startswith('>  <') or line.startswith('$$$$'):
                if name:
                    value = value.replace('\n', '').replace('\r', '')
                    # print "Name:%s Value:%s" % (name, value)
                    attr_list.append({'name':name, 'value':value})
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
            
            # print '---------------------'
            # print '[%s]' % line
            if check_line == '$$$$':
                # print attr_list
                # print mol
                # 已经完成对一个化合物数据的提取
                try:
                    v_d = {}
                    for key in dict_conf.SDF_KEY:
                        for attr in attr_list:
                            if attr['name'] in dict_conf.SDF_KEY[key]:
                                v_d[key] = attr['value']
                    query_mol_id = -1
                    if v_d['cas_no'] in self.fix_cas:
                        query_mol_id = self.write_dic(v_d, mol)
                    sql = 'select * from search_moldata where mol_id=%s'
                    sql = sql % query_mol_id
                    
                    rs = self.db_dict.query(sql)
                    for r in rs:
                        export_list.append((r['mol_id'], r['cas_no']))
                    for price in prices:
                        goods_list.append((r['mol_id'], r['cas_no'], goods_dict.get('PURITY', ''), goods_dict.get('LEAD_TIME', ''), goods_dict.get('STOCK', ''), goods_dict.get('CAPACITY', ''), price))
                except Exception, e:
                    logging.error(u"处理产品时出错:%s", attr_list)
                    logging.error(traceback.format_exc())
                attr_list = []
                mol = ''
                name = ''
                value = ''
                counter = 0
                goods_dict = {}
                prices = []
                # break
        fp_reader.close()
        
        file_name = fp[fp.rfind('/') + 1:fp.rfind('.')]
        self.write_csv(file_name, ['mol_id', 'cas_no'], export_list)
        self.write_csv(file_name + '_goods', ['mol_id', 'cas_no', 'PURITY', 'LEAD_TIME', 'STOCK', 'CAPACITY', 'price'], goods_list)
    
    def write_csv(self, file_name, columns, data_list):
        result_file_fp = dict_conf.SDF_RESULT_PATH + file_name + ".csv"
        self.delete_file(result_file_fp)
        csvfile = file(result_file_fp, 'wb')
        writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow(columns)
        writer.writerows(data_list)
        csvfile.close()
    
    def get_start_molid(self):
        sql = 'select max(mol_id) as mol_id from search_moldata'
        rs = self.db_dict.query(sql)
        for r in rs:
            mol_id = r['mol_id']
        if not mol_id:
            mol_id = 0
        return mol_id
    
    def write_dic(self, data_dict, mol):
        logging.info(u"处理属性:%s数据", data_dict)
        if not self.cu.cas_check(data_dict['cas_no']):
            return -1
        self.delete_file(self.tmp_mol1)
        mol1_writer = open(self.tmp_mol1, 'w')
        mol1_writer.write(mol)
        mol1_writer.close()
        check_mol_id = self.check_match(data_dict['cas_no'], mol)
        # 字典中有相应的数据
        if check_mol_id > 0:
            mol_id = check_mol_id
            return check_mol_id
        else:
            self.i_mol_id = self.i_mol_id + 1
            mol_id = self.i_mol_id
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
        params.append(data_dict['name_cn'])
        params.append(data_dict['name_cn_alias'])
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

        if check_mol_id < 0 :
            sql = '''INSERT INTO search_moldata (mol_id, mol_name, en_synonyms, zh_synonyms, name_cn, cas_no, 
                                                    formula,mol_weight,exact_mass,smiles,inchi,
                                                    num_atoms,num_bonds,num_residues,sequence,
                                                    num_rings,logp,psa,mr,goods_count) VALUES (
                                                    %s,%s,%s,%s,%s,%s,
                                                    %s,%s,%s,%s,%s,
                                                    %s,%s,%s,%s,
                                                    %s,%s,%s,%s,0
                                                    )'''
            logging.info(u"写入新数据,mol_id:%s!", mol_id)
            # logging.info(sql)
            self.db_dict.insert(sql, *params)
        else:
            sql = '''update search_moldata  set formula=%s,mol_weight=%s,exact_mass=%s,smiles=%s,inchi=%s,
                                                    num_atoms=%s,num_bonds=%s,num_residues=%s,sequence=%s,
                                                    num_rings=%s,logp=%s,psa=%s,mr=%s
                                                    where mol_id=%s
                                                    '''
            logging.info(u"更新数据,mol_id:%s!", mol_id)
            u_params = params[6:]
            u_params.append(mol_id)
            # print u_params
            self.db_dict.execute(sql, *u_params)
        self.delete_data(mol_id)
        # 对mol文件进行相应的格式化
        c = "echo \"%s\" | checkmol -m - 2>&1" % mol
        result = os.popen(c).read()
        # print "molformat>>>" + result
        sql = "insert into search_molstruc values ('%s','%s',0,0)"
        sql = sql % (mol_id, result)
        self.db_dict.insert(sql)
        c = "echo \"%s\" | checkmol -aXbH - 2>&1" % mol
        result = os.popen(c).read()
        # print result
        results = result.split("\n")
        molstat = results[0]
        molfgb = results[1]
        molhfp = results[2]
        if ('unknown' not in molstat) and ('invalid' not in molstat):
            sql = 'insert into search_molstat values (%s,%s)' % (mol_id, molstat)
            # logging.info(u"执行的sql:%s", sql)
            self.db_dict.insert(sql)
            molfgb = molfgb.replace(';', ',')
            sql = 'insert into search_molfgb values (%s,%s)' % (mol_id, molfgb)
            self.db_dict.insert(sql)
            
            molhfp = molhfp.replace(';', ',')
            sql = 'insert into search_molcfp values (%s,%s,%s)'
            cand = "%s$$$$%s" % (mol, self.fpdef)
            cand = cand.replace('$', '\$')
            c = "echo \"%s\" | %s -F - 2>&1" % (cand, dict_conf.MATCHMOL)
            result = os.popen(c).read().replace('\n', '')
            sql = sql % (mol_id, result, molhfp)
            self.db_dict.insert(sql)
        pic_path = str(mol_id)
        while len(pic_path) < 8:
            pic_path = '0' + pic_path
        pic_dir = pic_path[0:4]
        pic_dir = '%s/%s/%s.png' % (pic_dir[0:2], pic_dir[2:4], mol_id)
        pic_fp = dict_conf.worker_bitmapdir + '/' + pic_dir
        if not os.path.exists(pic_fp[0:pic_fp.rfind('/')]):
            os.makedirs(pic_fp[0:pic_fp.rfind('/')])
        self.delete_file(pic_fp)
        # print pic_fp
        # print pic_dir
        c = "echo \"%s\" | %s %s - 2>&1"
        c = c % (mol, dict_conf.MOL2PS, dict_conf.mol2psopt)
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
        status = 1
        # print 'pic_size>>' + result
        sql = "insert into search_pic2d (mol_id,type,status,s_pic,s_width,s_height) values ('%s',1,'%s','%s','%s','%s')"
        sql = sql % (mol_id, status, pic_dir, pic_width, pic_height)
        # print sql
        self.db_dict.insert(sql);
        return mol_id
    
    def check_match(self, cas_no, mol):
        sql = 'select * from search_moldata where cas_no=%s order by mol_id asc'
        rs = self.db_dict.query(sql, cas_no)
        for r in rs:
            return r['mol_id']
        c = "echo \"%s\" | checkmol -axH -" % mol
        # logging.info(c)
        result = os.popen(c).read()
        # logging.info(u"check_mol_result:%s", result)
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
            return -1
        
        for r in rs:
            self.delete_file(self.tmp_mol2)
            mol2_writer = open(self.tmp_mol2, 'w')
            mol2_writer.write(r['struc'])
            mol2_writer.close()
            c = "%s -aisxgG %s %s" % (dict_conf.MATCHMOL, self.tmp_mol1, self.tmp_mol2)
            result = os.popen(c).read()
            # 返回相应的molid
            if ':T' in result:
                return r['mol_id']
        pass
    
    def delete_data(self, mol_id):
        sql = 'delete from search_molstruc where mol_id=%s'
        sql = sql % mol_id
        self.db_dict.execute(sql)
        sql = 'delete from search_pic2d where mol_id=%s'
        sql = sql % mol_id
        self.db_dict.execute(sql)
        self.delete_stat_table(mol_id)
    
    def delete_stat_table(self, mol_id):
        sql = 'delete from search_molstat where mol_id=%s'
        sql = sql % mol_id
        self.db_dict.execute(sql)
        sql = 'delete from search_molfgb where mol_id=%s'
        sql = sql % mol_id
        self.db_dict.execute(sql)
        sql = 'delete from search_molcfp where mol_id=%s'
        sql = sql % mol_id
        self.db_dict.execute(sql)
        
    def update_stat_table_db(self):
        counter = 0
        while 1:
            if counter > 10000:
                logging.info(u'循环次数过多,退出')
                break 
            sql = 'select max(mol_id) as mol_id from search_molstat'
            rs = self.db_dict.query(sql)
            mol_id = 0
            for r in rs:
                mol_id = r['mol_id']
                if not mol_id:
                    mol_id = 0
            sql = 'select * from search_molstruc where mol_id>%s limit 1000'
            rs = self.db_dict.query(sql, mol_id)
            logging.info(u"需要修正的加速表的数据量为:%s 起始mol_id:%s", len(rs), mol_id)
            for r in rs:
                self.update_stat_table(r['mol_id'], r['struc'])
            counter += 1
    
    def update_stat_table_fix(self):
        fix_mol_ids = [45]
        fix_mol_ids = str(fix_mol_ids).replace('L', '')
        # print fix_mol_ids
        sql = 'select * from search_molstruc where mol_id in (%s)' % fix_mol_ids[1:len(fix_mol_ids) - 1]
        rs = self.db_dict.query(sql)
        for r in rs:
            self.update_stat_table(r['mol_id'], r['struc'])
    
    def import_table_data(self):
        sql = 'select * from dic_source_data'
        rs = self.db_dict_source.query(sql)
        for r in rs:
            try:
                signal.alarm(10)
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
                # logging.info(u'生成mol的')
                self.write_dic(data_dict, result)
            except Exception, e:
                logging.error(u"处理cas:%s 产品:%s ErrMsg:%s", r['cas_no'], r['name_en'], e)
                logging.error(traceback.format_exc())
            # break
    
    def check_data_exist(self, cas):
        sql = "select * from search_moldata where cas_no='%s'"
        sql = sql % cas
        _rs = self.db_dict.query(sql)
        if len(_rs) > 0:
            logging.info(u'cas:%s 数据已经存在', cas)
            return True
        return False

if __name__ == '__main__':

    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="/tmp/data_fix.log", help="NSQ topic")
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
    worker = DictWorker()
    # worker.update_stat_table_fix()
    worker.update_stat_table_db()
    logging.info(u'程序运行完成')
