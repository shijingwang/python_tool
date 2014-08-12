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
import settings
from html_format import HtmlFormat

class ExtractData(object):
    
    def __init__(self):
        self.company_type = {
                    13:'synthonix',
                    23:'arkpharminc',
                    43:'tcichemicals',
                    53:'sigmaaldrich',
                    63:'acros',
                    74:'guidechem'         
                    }
        self.db_spider_data = ConUtil.connect_mysql(settings.MYSQL_SPIDER_DATA)
        self.db_molbase = ConUtil.connect_mysql(settings.MYSQL_MOLBASE)
        self.html_fomrat = HtmlFormat()
        pass
    
    def extract_all_data(self):
        counter = 0
        while True:
            counter += 1
            if counter >= 10000:
                logging.info("循环次数过多，退出")
                break
            result = self.extract_data()
            if result == 0:
                break
            
    
    def extract_data(self):
        size = 1000
        sql = 'select * from mark where type=1'
        # mark data set
        mds = self.db_spider_data.query(sql)
        if len(mds) == 0:
            mv = '0'
        else:
            mv = mds[0]['value']
        sql = 'select * from file_download where id>%s and status=1 order by id asc limit %s'
        sql = sql % (mv, size)
        logging.info(u"sql语句为:%s", sql)
        # download data
        ddata = self.db_spider_data.query(sql)
        if len(ddata) == 0:
            return 0
        # storage set
        ss = set()
        # source set
        source_set = set()
        for d in ddata:
            ss.add(str(d['storage_key']))
            if d['source_key'] != None:
                source_set.add(str(d['source_key']))
        query_keys = str(ss)
        query_keys = query_keys.replace("[", "").replace("]", "").replace("set", "")
        
        query_source_keys = str(source_set)
        query_source_keys = query_source_keys.replace("[", "").replace("]", "").replace("set", "")
        cp_data = []
        if len(query_source_keys) > 0 and len(source_set) > 0:
            sql = 'select * from compound_product where _key in %s' % query_source_keys
            # logging.info(sql)
            cp_data = self.db_spider_data.query(sql)
            logging.info(u'查出产品的数据大小为:%s', len(cp_data))
            for cd in cp_data:
                _value = json.loads(cd['_value'])
                cd['url'] = _value['url']
        sql = 'select * from file_info where _key in %s' % query_keys
        # logging.info(sql)
        data = self.db_spider_data.query(sql)
        logging.info(u"查询出的文件数量为:%s", len(data))
        for d in ddata:
            logging.info(u"处理编号:%s MSDS文档", d['id'])
            try:
                file_type = int(d['task_type'])
                cas = d['name3'].strip()
                language = d['language']
                source_key = d['source_key']
                url = ''
                path = ''
                for d1 in data:
                    if d1['_key'] == d['storage_key']:
                        path = d1['path']
                        break
                if path == 0:
                    continue
                for d2 in cp_data:
                    if source_key == d2['_key']:
                        url = d2['url']
                        break
                if file_type == 74:
                    if language == 'English':
                        content = self.html_fomrat.en_format(settings.MSDS_FILE_PATH + path)
                    else:
                        content = self.html_fomrat.cn_format(settings.MSDS_FILE_PATH + path)
                    sql = "insert into search_msdsv2 (cas,language,check_key,content) values (%s,%s,%s,%s)"
                    # logging.info(sql)
                    # self.db_molbase.execute(sql)
                    self.db_molbase.insertmany(sql, [(cas, language, d['_key'], content)])
                else:
                    if url == None or url == '':
                        raise Exception('5', u'没有URL')
                    sql = "insert into search_msdsv2 (cas,language,check_key,link_name,link) values ('%s','%s','%s','%s','%s')" % (cas, language, d['_key'], self.company_type[file_type], url)
                    # logging.info(sql)
                    self.db_molbase.execute(sql)
            except Exception, e:
                logging.error(u'提取数据时出错:%s  %s', d['id'], e)
                logging.error(traceback.format_exc())
                break
            finally:
                sql = "insert into mark (type,value) values (1, '%s') on duplicate key update value='%s'"
                sql = sql % (d['id'], d['id'])
                self.db_spider_data.execute(sql)
        return 1

if __name__ == '__main__':

    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="/tmp/msds_extract.log", help="NSQ topic")
    define("func_name", default="spider_apple")
    options.parse_command_line()
    logfile = options.logfile
    fmt = '%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s'
    handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=100 * 1024 * 1024, backupCount=10)  # 实例化handler
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt)  # 实例化formatter
    handler.setFormatter(formatter)  # 为handler添加formatter
    logging.getLogger('').addHandler(handler)
    logging.info(u'写入的日志文件为:%s', logfile)
    ed = ExtractData()
    ed.extract_all_data()
    logging.info(u'程序运行完成')
