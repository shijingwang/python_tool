# coding=utf-8

import logging
import traceback
import pymssql
import _mssql
from tornado.options import define, options

class MSSQLExtract(object):
    
    def __init__(self):
        dev_env = {
                "server":"devdb.dev.sh.ctriptravel.com",
                "port":"28747",
                "user":"uws_AllInOneKey_dev",
                "password":"!QAZ@WSX1qaz2wsx",
                "database":"HtlVendorPriceDB"
            }
        prod_env = {
                "server":"HtlVendorPrice.db.sh.ctripcorp.com",
                "port":"55944",
                "user":"uws_M_HawthornHtlVendorPrice",
                "password":"0sISYsTBX5CVGeNQ==",
                "database":"HtlVendorPriceDB"
            }
        self.db_server = dev_env
    
    def select(self):
        logging.info(u"开始查询数据...")
        hotel_id_list=['50201238','50201008','50201013','40201020','40201044','40201161','30201066','30201086','40201049','20201367','20201329','00201318']
        try:
            conn = pymssql.connect(host=self.db_server['server'], user=self.db_server['user'], password=self.db_server["password"], database=self.db_server["database"], port=self.db_server["port"], login_timeout=10, charset="UTF-8")
            cursor = conn.cursor(as_dict=True)
            min_id = 3318329431
            max_id = 3353698803
            min_id = 22399739
            max_id = 22400193
            start_id = min_id
            step = 1000
            while True:
                stop_id = start_id + step
                logging.info(u"查询范围为数据:%s至%s数据", start_id, stop_id); 
                cursor.execute('select CrawlerResultId,HotelID from CrawlerResult_ELong where CrawlerResultId>=%s and CrawlerResultId<%s' % (start_id, stop_id))
                for row in cursor:
                    print(row['HotelID'] in hotel_id_list)
                if stop_id > max_id:
                    break
                start_id = start_id + step
            
        except Exception, e:
            logging.error(u"连接数据库出错:%s", str(e))
            logging.error(traceback.format_exc())
        finally:
            conn.close()
        logging.info(u"完成数据查询!")
        
        
if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="D:/Log/py.log", help="NSQ topic")
    define("func_name", default="display_calc_job")
    options.parse_command_line()
    
    ms = MSSQLExtract();
    ms.select()
