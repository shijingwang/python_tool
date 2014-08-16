# -*- coding: utf-8 -*-
from tornado.options import define, options
import logging
import os, sys
import csv
import re
import traceback
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
        self.db_dict_source = ConUtil.connect_mysql(dict_conf.MYSQL_DICT_SOURCE)
        if not os.path.exists(dict_conf.bitmapdir):
            os.makedirs(dict_conf.bitmapdir)
        self.tmp_mol1 = '/tmp/mol1.mol';self.tmp_mol2 = '/tmp/mol2.mol'
        self.i_mol_id = self.get_start_molid()
        self.cas_p = re.compile(r'([0-9]{2,7})[-—]{1}([0-9]{2})[-—]{1}([0-9]{1})')
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

    def import_sdf(self, sdf_file):
        fp = sdf_file
        logging.info("导入sdf文档数据:%s", fp)
        fp_reader = open(fp)
        mol = ''
        name = ''
        value = ''
        attr_list = []
        export_list = []
        counter = 0
        while 1:
            line = fp_reader.readline()
            if not line:
                counter += 1
                if counter >= 20:
                    break
            # print '=======' + line
            if line.startswith('>  <'):
                if name:
                    value = value.replace('\n', '').replace('\r', '')
                    # print "Name:%s Value:%s" % (name, value)
                    attr_list.append({'name':name, 'value':value})
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
                    query_mol_id = self.write_dic(v_d, mol)
                    sql = 'select * from search_moldata where mol_id=%s'
                    sql = sql % query_mol_id
                    rs = self.db_dict.query(sql)
                    for r in rs:
                        export_list.append((r['mol_id'], r['cas_no']))
                except Exception, e:
                    logging.error(u"处理产品时出错:%s", attr_list)
                    logging.error(traceback.format_exc())
                attr_list = []
                mol = ''
                name = ''
                value = ''
                counter = 0
                # break
        fp_reader.close()
        file_name = fp[fp.rfind('/') + 1:fp.rfind('.')]
        result_file_fp = dict_conf.SDF_RESULT_PATH + file_name + ".csv"
        csvfile = file(result_file_fp, 'wb')
        writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow(['mol_id', 'cas_no'])
        writer.writerows(export_list)
        csvfile.close()
        pass
    
    def get_start_molid(self):
        sql = 'select max(mol_id) as mol_id from search_moldata'
        rs = self.db_dict.query(sql)
        for r in rs:
            mol_id = r['mol_id']
        if not mol_id:
            mol_id = 0
        mol_id += 1
        return mol_id
    
    def write_dic(self, data_dict, mol):
        logging.info(u"处理属性:%s数据", data_dict)
        self.delete_file(self.tmp_mol1)
        mol1_writer = open(self.tmp_mol1, 'w')
        mol1_writer.write(mol)
        mol1_writer.close()
        check_mol_id = self.check_match(mol)
        # 字典中有相应的数据
        if check_mol_id > 0:
            mol_id = check_mol_id
            # return check_mol_id
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
            #logging.info(sql)
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
        pic_fp = dict_conf.bitmapdir + '/' + pic_dir
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
    
    def check_match(self, mol):
        c = "echo \"%s\" | checkmol -axH -" % mol
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
        # logging.info(u"执行的sql:%s", sql)
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
        
    def update_stat_table(self):
        sql = 'select * from search_molstruc'
        rs = self.db_dict.query(sql)
        logging.info(u"需要修正的加速表的数据量为:%s", len(rs))
        for r in rs:
            c = "echo \"%s\" | %s -aXbHs 2>&1" % (r['struc'], dict_conf.CHECKMOL_V2)
            result = os.popen(c).read()
            results = result.split("\n")
            self.insert_stat_table(r['mol_id'], r['struc'], results)
    
    # 新指令更新加速表的数据, 需要和PHP同步修改
    def insert_stat_table(self, mol_id, mol, results):
        molstat = results[0]
        molfgb = results[1]
        molhfp = results[2]
        if ('unknown' in molstat) or ('invalid' in molstat):
            logging.info(u"更新mol_id:%s加速表时，指令返回的结果错误", mol_id)
            return
        logging.info(u"更新mol_id:%s 加速表数据", mol_id)
        self.delete_stat_table(mol_id)
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
    
    def import_table_data(self):
        sql = 'select * from dic_source_data'
        rs = self.db_dict_source.query(sql)
        for r in rs:
            # logging.info(u'处理id:%s cas_no:%s的记录', r['id'], r['cas_no'])
            if not r['cas_no']:
                logging.info(u'id:%s 记录无cas号', r['id'])
                continue
            if not self.cas_check(r['cas_no']):
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
            result = os.popen(c).read()
            self.write_dic(data_dict, result)
            # break
    
    def check_data_exist(self, cas):
        sql = "select * from search_moldata where cas_no='%s'"
        sql = sql % cas
        _rs = self.db_dict.query(sql)
        if len(_rs) > 0:
            logging.info(u'cas:%s 数据已经存在', cas)
            return True
        return False
    
    def cas_check(self, cas):
        match = self.cas_p.match(cas)
        if not match:
            return False
        cas1 = match.group(1)
        cas2 = match.group(2)
        cas3 = int(match.group(3))
        check_cas = cas1 + cas2
        check_cas = check_cas[::-1]
        counter = 0
        total = 0
        for c in check_cas:
            total = total + int(c) * (counter + 1)
            counter += 1
        if total % 10 == cas3:
            return True
        return False
    
if __name__ == '__main__':

    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="/tmp/sdf_import.log", help="NSQ topic")
    define("func_name", default="spider_apple")
    define("sdf_file", default="/home/kulen/PerlProject/python_tool/dict_data/test.sdf")
    options.parse_command_line()
    logfile = options.logfile
    sdf_file = options.sdf_file
    fmt = '%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s'
    handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=100 * 1024 * 1024, backupCount=10)  # 实例化handler
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt)  # 实例化formatter
    handler.setFormatter(formatter)  # 为handler添加formatter
    logging.getLogger('').addHandler(handler)
    logging.info(u'写入的日志文件为:%s', logfile)
    worker = SdfWorker()
    worker.import_sdf(sdf_file)
    # worker.update_stat_table()
    # worker.import_table_data()
    # worker.test()
    logging.info(u'程序运行完成')
