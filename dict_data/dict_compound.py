# -*- coding: utf-8 -*-
from tornado.options import define, options
import logging
import os, sys
import traceback
import json
try:
    import python_tool
except ImportError:
    fp = os.path.abspath(__file__)
    sys.path.append(fp[0:fp.rfind('python_tool') + 11])
from common.con_util import ConUtil
from common.cas_util import CasUtil
from common.file_util import FileUtil
import dict_conf
import CK

class DictCompound(object):
    
    def __init__(self):
        self.redis_server = ConUtil.connect_redis(dict_conf.REDIS_SERVER)
        self.tmp_mol1 = '/tmp/mol1.mol';self.tmp_mol2 = '/tmp/mol2.mol'
        self.cu = CasUtil()
        self.fu = FileUtil()
        
    def get_write_molid(self):
        sql = 'select max(mol_id) as mol_id from search_moldata'
        rs = self.db_dict.query(sql)
        for r in rs:
            mol_id = r['mol_id']
        if not mol_id:
            mol_id = 0
        mol_id += 1
        return mol_id
    
    def check_match(self, cas_no, mol):
        if cas_no and self.cu.cas_check(cas_no):
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
        # logging.info(u"执行的sql:%s", sql)
        rs = self.db_dict.query(sql)
        if len(rs) == 0:
            return -1
        
        for r in rs:
            self.fu.delete_file(self.tmp_mol2)
            mol2_writer = open(self.tmp_mol2, 'w')
            mol2_writer.write(r['struc'])
            mol2_writer.close()
            c = "%s -aisxgG %s %s" % (dict_conf.MATCHMOL, self.tmp_mol1, self.tmp_mol2)
            result = os.popen(c).read()
            # 返回相应的molid
            if ':T' in result:
                logging.info(u'cas_no:%s 和 mol_id:%s 指向同一个产品', cas_no, r['mol_id'])
                return r['mol_id']
        return -1
    
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
    
    def read_sql(self, redis_msg, mol_id):
        sql = 'select * from search_moldata where mol_id=%s'
        columns = ['mol_id', 'mol_name', 'en_synonyms', 'zh_synonyms', 'name_cn', 'cas_no', 'formula', 'mol_weight', 'exact_mass', 'smiles', 'inchi', 'num_atoms', 'num_bonds', 'num_residues', 'sequence', 'num_rings', 'logp', 'psa', 'mr', 'is_user_add', 'is_audit']
        rs = self.db_dict.query(sql, mol_id)
        for r in rs:
            for column in columns:
                sql += '%s,'
                if column == 'formula':
                    redis_msg['formula'] = r[column]
                if column == 'cas_no':
                    redis_msg['cas_no'] = r[column]
        redis_msg['mol_id'] = mol_id
        self.generate_sql(redis_msg, 'search_moldata', mol_id, filter_columns=columns)
        if 'search_moldata' in redis_msg:
            redis_msg['search_moldata']['type'] = 'insert'
        else:
            logging.error(u'mol_id:%s moldata中没有相应的数据', mol_id)
            return
        self.generate_sql(redis_msg, 'search_molstruc', mol_id)
        self.generate_sql(redis_msg, 'search_molstat', mol_id)
        self.generate_sql(redis_msg, 'search_molfgb', mol_id)
        self.generate_sql(redis_msg, 'search_molcfp', mol_id)
        self.generate_sql(redis_msg, 'search_pic2d', mol_id)
    
    def generate_sql(self, redis_msg, table_name, mol_id, filter_columns=[]):
        sql = 'select * from ' + table_name + ' where mol_id=%s'
        rs = self.db_dict.query(sql, mol_id)
        params = []
        columns = []
        if len(rs) != 1:
            return
        for r in rs:
            for key in r.keys():
                if r[key] == None:
                    continue
                if len(filter_columns) > 0: 
                    if key in filter_columns:
                        params.append(str(r[key]))
                        columns.append(key)
                else:
                    params.append(str(r[key]))
                    columns.append(key)
        if len(columns) == 0:
            return
        sql = 'insert into %s (%s) values (%s)'
        sql = sql % (table_name, str(columns).replace('[', '').replace(']', '').replace("'", ""), '%s,' * len(columns))
        sql = sql.replace(',)', ')')
        redis_msg[table_name] = {'sql':sql, 'params':params}
        # print sql
        return sql

    def write_json_data(self, mol_id, dict_j):
        logging.info(u'写入mol_id:%s  操作类型:%s 的数据', mol_id, dict_j['search_moldata']['type'])
        for tb_name in ['search_moldata', 'search_molstruc', 'search_pic2d', 'search_molstat', 'search_molfgb', 'search_molcfp']:
            if tb_name in dict_j:     
                self.db_dict.insert(dict_j[tb_name]['sql'], *dict_j[tb_name]['params'])
            else:
                logging.info(u'mol_id:%s 无表:%s 数据', mol_id, tb_name)
        
        
