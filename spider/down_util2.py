# -*- coding: utf-8 -*-
import urllib, urllib2
import time
import logging
import traceback
from tornado.options import define, options


class DownUtil2(object):

    def __init__(self):
        pass

    def downfile(self, url, data, storage_path, file_name):
        start_time = int(time.time())
        logging.info(u'开始下载URL:%s的数据', url)
        result = {'code':0, 'path':''}
        try:
            
            data = urllib.urlencode(data)  
            req = urllib2.Request(url, data)
            response = urllib2.urlopen(req, timeout=20)
            if response.code != 200:
                raise Exception(u'http状态不出错', response.code)
            url = response.url
            ctype = response.headers.getheader('Content-Type')
            clength = response.headers.getheader('Content-Length')
            if clength == None or len(clength.strip()) == 0:
                clength = '1'
            clength = int(clength)
            storage_path = storage_path + file_name
            result['path'] = storage_path
            logging.info(u'URL地址:%s  存储路径:%s  文件大小为:%sM  文件类型:%s', url, storage_path, round(clength / (1024.0 * 1024), 2), ctype)
            f = open(storage_path, 'wb')
            file_size_dl = 0
            block_size = 1024 * 5
            i = 0
            while True:
                i += 1
                fbuffer = response.read(block_size)
                if not fbuffer:
                    break
                file_size_dl += len(fbuffer)
                f.write(fbuffer)
                progress = round(file_size_dl * 1.0 / clength, 4) * 100
                if i % 1000 == 0:
                    logging.debug(u'url地址:%s 下载进度为:%.2f%%', url, progress)
        except Exception:
            result['code'] = 4
            logging.info(u'URL地址:%s 下载时出错:%s', url, traceback.format_exc())
        finally:
            try:
                f.close()
            except Exception:
                pass
            try:
                response.close()
            except Exception:
                pass
        stop_time = int(time.time())
        logging.info(u'URL地址: %s 下载用时: %s秒', url, stop_time - start_time)
        return result

if __name__ == '__main__':
   
    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')

    define("logfile", default="F:/Log/py.log", help="NSQ topic")
    options.parse_command_line()
    logfile = options.logfile
    logging.info(u'写入的日志文件为:%s', logfile)
    # 自动对日志文件进行分割
    fmt = '%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s'
    handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=100 * 1024 * 1024, backupCount=10)  # 实例化handler
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt)  # 实例化formatter
    handler.setFormatter(formatter)  # 为handler添加formatter
    logging.getLogger('').addHandler(handler)

    du = DownUtil2()
    du.downfile('http://img.hb.aicdn.com/3eef1e72014852acecb9f660a63d3f367c026864515c5-WEYNJA_fw658', {}, 'F:/file/', '.jpg')
