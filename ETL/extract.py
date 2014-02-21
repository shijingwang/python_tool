# coding=utf-8

import logging
import traceback
import pymssql
import _mssql

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
                "server":"devdb.dev.sh.ctriptravel.com",
                "port":"28747",
                "user":"uws_AllInOneKey_dev",
                "password":"!QAZ@WSX1qaz2wsx",
                "database":"HtlVendorPriceDB"
            }
        self.db_server = dev_env
    
    def select(self):
        try:
            conn = pymssql.connect(host=self.db_server['server'], user=self.db_server['user'], password=self.db_server["password"], database=self.db_server["database"], port=self.db_server["port"], login_timeout=10, charset="UTF-8")
            cursor = conn.cursor(as_dict=True)
            cursor.execute('select top 10 * from CrawlerResult_ELong')
            for row in cursor:
                print(row["HotelName"])

        except Exception, e:
            logging.error("连接数据库出错:%s", str(e))
            logging.traceback()
        finally:
            conn.close()
        
if __name__ == "__main__":
    ms = MSSQLExtract();
    ms.select()
