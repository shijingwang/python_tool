# -*- coding: utf-8 -*-
from tornado.options import define, options
import logging
import os
import Image, ImageDraw, ImageFont
import math

from common.con_util import ConUtil
import settings

class Extract(object):
    
    def __init__(self):
        self.molbase_db = ConUtil.connect_mysql(settings.MYSQL_MOLBASE_DEV)
        self.file_list = []
        
    
    def get_data(self, days):
        for f in self.file_list:
            fp = f[len(settings.NMR_FILE_PATH + days):]
            ftype = 1 if '1h' in fp else 2
            molid = fp[:fp.rfind('/')]
            molid = molid.replace('/', '')
            molid = int(molid)
            cas = fp[fp.rfind('/') + 1:fp.rfind('-')]
            sql = "insert into search_nmr (mol_id,type,cas,path) values (%s,%s,'%s','%s')"
            sql = sql % (molid, ftype, cas, fp)
            self.molbase_db.insert(sql)
    
    def mark_all_image(self):
        for f in self.file_list:
            target = f.replace(".png", ".mark.png")
            try:
                self.image_mark(f, target)
                os.rename(target,f)  
            except Exception:
                pass
            
    def delete_file(self, fp):
        try:
            if os.path.exists(fp):
                os.remove(fp)
        except Exception:
            pass

    def image_mark(self, source, target):
        fileName = source
        logoName = "/home/kulen/tmp/logo_mark_40.png"
        logging.info(u'图片打水印:%s', fileName)
        im = Image.open(fileName)
        mark = Image.open(logoName)
        imWidth, imHeight = im.size
        markWidth, markHeight = mark.size
        #print im.size
        #print mark.size
        if im.mode != 'RGBA':  
            im = im.convert('RGBA')
        if mark.mode != 'RGBA':  
            mark = mark.convert('RGBA')
        layer = Image.new('RGBA', im.size, (0, 0, 0, 0))  
        x = 0
        y = 0
        while y < imHeight:
            x = 0
            if y == 0:
                y += 150
            else:
                y += 300
            while x < imWidth:
                if x == 0:
                    x += 250
                else:
                    x += 500
                if x > imWidth:
                    continue
                if x + markWidth + 150 > imWidth:
                    continue
                if y + markHeight + 80 > imHeight:
                    continue
                layer.paste(mark, (x, y))  
        Image.composite(layer, im, layer).save(target, quality=90)
        logging.info(u'图片完成打水印:%s', fileName)
    
    def extract_pdf_data(self):
        pass
    
    def list_file_dir(self, level, path):  
        for i in os.listdir(path):
            os.path.isdir(path + '/' + i)  
            if os.path.isdir(path + '/' + i):  
                self.list_file_dir(level + 1, path + '/' + i)
            else:
                self.file_list.append(path + '/' + i)

if __name__ == '__main__':

    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="/home/kulen/log/run.log", help="NSQ topic")
    define("func_name", default="spider_apple")
    options.parse_command_line()
    logfile = options.logfile
    func_name = options.func_name
    logging.info(u'写入的日志文件为:%s', logfile)
    
    extract = Extract()
    # extract.get_data('/home/kulen/NmrMsdsETL/2014-07-17/')
    extract.list_file_dir(1, "/home/kulen/NmrMsdsETL/2014-07-17")
    # extract.get_data('2014-07-17')
    # extract.image_mark()
    extract.mark_all_image()
    logging.info(u'程序运行完成')
    # print os.listdir("/home/kulen/NmrMsdsETL/2014-07-17/000/000/014")
    
