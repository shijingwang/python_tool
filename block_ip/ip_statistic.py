# coding=utf-8

import logging
import traceback
import MySQLdb
from common.con_util import ConUtil
from tornado.options import define, options
import xlwt

class IpStatistic(object):
    
    def __init__(self):
        dev_env = {
                "host":"pub.mysql.db.dev.sh.ctripcorp.com",
                "port":"28747",
                "user":"uapp_crawler_r_1",
                "password":"YmAd0ym8triZaY4UA1z2",
                "database":"crawlerresultmdb"
            }
        prod_env = {
                "host":"10.8.3.144",
                "port":"8080",
                "user":"uapp_crawler_r",
                "password":"WXzHvxCjYClbP1wLjUMx",
                "database":"crawlerresultmdb"
            }
        self.db_server = dev_env
        self.con = ConUtil.connect_mysql(prod_env)
    
    def select(self):
        logging.info(u"开始查询数据...")
        start_time = '2014-03-17 00:00'
        stop_time = '2014-03-20 00:00'
        city_dict = {'sh':'上海', 'gz':'广州', 'nj':'南京', 'nt':'南通', 'sz':'深圳', 'cd':'成都'}
        ip_total_sql = "select distinct(clientIp) from octopus_exception where clientName like '%%%%%s%%%%' and  length(clientIp)>0 and lastModified>unix_timestamp('%s')*1000 and lastModified<unix_timestamp('%s')*1000 order  by clientIp desc"
        b_ip_sql = "select distinct(SUBSTRING_INDEX(clientIp,'.',2)) as b_ip from octopus_exception where clientName like '%%%%%s%%%%' and  length(clientIp)>0 and lastModified>unix_timestamp('%s')*1000 and lastModified<unix_timestamp('%s')*1000 order by clientIp desc"
        c_ip_sql = "select distinct(SUBSTRING_INDEX(clientIp,'.',3)) from octopus_exception where clientName like '%%%%%s%%%%' and  length(clientIp)>0 and lastModified>unix_timestamp('%s')*1000 and lastModified<unix_timestamp('%s')*1000 order by clientIp desc"
        repeat_sql = "select clientIp,count(*) as num from octopus_exception where clientName like '%%%%%s%%%%' and  length(clientIp)>0 and lastModified>unix_timestamp('%s')*1000 and lastModified<unix_timestamp('%s')*1000 group by clientIp having count(*)>1 order by num desc;"
        elong_block_ip_sql = "select distinct(clientIp) from octopus_exception where errorCode in (306) and clientName like '%%%%%s%%%%' and  length(clientIp)>0 and lastModified>unix_timestamp('%s')*1000 and lastModified<unix_timestamp('%s')*1000 order by clientIp desc;"
        # 306
        qunar_block_ip_sql = "select distinct(clientIp) from octopus_exception where errorCode in (253) and clientName like '%%%%%s%%%%' and  length(clientIp)>0 and lastModified>unix_timestamp('%s')*1000 and lastModified<unix_timestamp('%s')*1000 order by clientIp desc;"
        # 253
        elong_block_ip_sql = qunar_block_ip_sql
        book = xlwt.Workbook(encoding='utf-8', style_compression=0)
        sheet = book.add_sheet(sheetname="ip_block", cell_overwrite_ok=True)
        sheet.write(0, 0, '序号')
        sheet.write(0, 1, '城市')
        sheet.write(0, 2, 'IP地址总数')
        sheet.write(0, 3, '重复IP地址量')
        sheet.write(0, 4, 'Block IP')
        sheet.write(0, 5, 'B段地址')
        sheet.write(0, 6, 'C段条数')
        counter = 1;
        for city in city_dict.keys():
            sheet.write(counter, 0, counter)
            sheet.write(counter, 1, city_dict[city])
            logging.info("城市数据统计:%s", city)
            _ip_total_sql = ip_total_sql % (city, start_time, stop_time)
            result = self.con.query(_ip_total_sql)
            logging.info("ip总数:%s", len(result))
            sheet.write(counter, 2, len(result))
            _b_ip_sql = b_ip_sql % (city, start_time, stop_time)
            result = self.con.query(_b_ip_sql)
            b_ip = ''
            r_size = len(result)
            _counter=1;
            for record in result:
                if _counter < r_size:
                    b_ip = b_ip + record['b_ip'] + '\n'
                else:
                    b_ip = b_ip + record['b_ip']
                _counter = _counter + 1
            logging.info("b网段数据:%s", b_ip)
            sheet.write(counter, 5, b_ip)
            _c_ip_sql = c_ip_sql % (city, start_time, stop_time)
            result = self.con.query(_c_ip_sql)
            logging.info("c网段总数:%s", len(result))
            sheet.write(counter, 6, len(result))
            _repeat_sql = repeat_sql % (city, start_time, stop_time)
            result = self.con.query(_repeat_sql)
            logging.info("重复IP地址总数:%s", len(result))
            sheet.write(counter, 3, len(result))
            _elong_block_ip_sql = elong_block_ip_sql % (city, start_time, stop_time)
            result = self.con.query(_elong_block_ip_sql)
            logging.info("Elong block IP地址总数:%s", len(result))
            sheet.write(counter, 4, len(result))
            counter = counter + 1
        book.save("D:/ip_block_q.xls")
        logging.info(u"完成数据查询!")
        
        
if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="D:/Log/py.log", help="NSQ topic")
    define("func_name", default="display_calc_job")
    options.parse_command_line()
    
    iss = IpStatistic();
    iss.select()
