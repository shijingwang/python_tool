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
            self.fu.delete_file(self.tmp_mol2)
            mol2_writer = open(self.tmp_mol2, 'w')
            mol2_writer.write(r['struc'])
            mol2_writer.close()
            c = "%s -aisxgG %s %s" % (dict_conf.MATCHMOL, self.tmp_mol1, self.tmp_mol2)
            result = os.popen(c).read()
            # 返回相应的molid
            if ':T' in result:
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
        columns = ['mol_id', 'mol_name', 'en_synonyms', 'zh_synonyms', 'name_cn', 'cas_no', 'formula', 'mol_weight', 'exact_mass', 'smiles', 'inchi', 'num_atoms', 'num_bonds', 'num_residues', 'sequence', 'num_rings', 'logp', 'psa', 'mr']
        rs = self.db_dict.query(sql, mol_id)
        sql = 'insert into search_moldata (%s) values ('
        sql = sql % str(columns)[1:len(str(columns)) - 1]
        sql = sql.replace("'", "")
        params = []
        for r in rs:
            for column in columns:
                params.append(str(r[column]))
                sql += '%s,'
                if column == 'formula':
                    redis_msg['formula'] = r[column]
                if column == 'cas_no':
                    redis_msg['cas_no'] = r[column]
        sql = sql[:len(sql) - 1]
        sql += ')'
        redis_msg['mol_id'] = mol_id
        redis_msg['search_moldata'] = {'sql':sql, 'params':params, 'type':'insert'}
        self.generate_sql(redis_msg, 'search_molstruc', mol_id, columns)
        self.generate_sql(redis_msg, 'search_molstat', mol_id, columns)
        self.generate_sql(redis_msg, 'search_molfgb', mol_id, columns)
        self.generate_sql(redis_msg, 'search_molcfp', mol_id, columns)
        self.generate_sql(redis_msg, 'search_pic2d', mol_id, columns)
    
    def generate_sql(self, redis_msg, table_name, mol_id, columns):
        sql = 'select * from ' + table_name + ' where mol_id=%s'
        rs = self.db_dict.query(sql, mol_id)
        params = []
        columns = []
        for r in rs:
            for key in r.keys():
                params.append(str(r[key]))
                columns.append(key)
        sql = 'insert into %s (%s) values (%s)'
        sql = sql % (table_name, str(columns).replace('[', '').replace(']', '').replace("'", ""), '%s,' * len(columns))
        sql = sql.replace(',)', ')')
        redis_msg[table_name] = {'sql':sql, 'params':params}
        # print sql
        return sql

    def write_json_data(self, mol_id, dict_j):
        logging.info(u'写入mol_id:%s  操作类型:%s 的数据', mol_id, dict_j['search_moldata']['type'])       
        self.db_dict.insert(dict_j['search_moldata']['sql'], *dict_j['search_moldata']['params'])
        self.db_dict.insert(dict_j['search_molstruc']['sql'], *dict_j['search_molstruc']['params'])
        self.db_dict.insert(dict_j['search_molstat']['sql'], *dict_j['search_molstat']['params'])
        self.db_dict.insert(dict_j['search_molfgb']['sql'], *dict_j['search_molfgb']['params'])
        self.db_dict.insert(dict_j['search_molcfp']['sql'], *dict_j['search_molcfp']['params'])
        self.db_dict.insert(dict_j['search_pic2d']['sql'], *dict_j['search_pic2d']['params'])
        
