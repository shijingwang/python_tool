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
        self.molbase_db = ConUtil.connect_mysql(settings.MYSQL_MOLBASE)
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
                os.rename(target, f)  
            except Exception:
                pass
            
    def delete_file(self, fp):
        try:
            if os.path.exists(fp):
                os.remove(fp)
        except Exception:
            pass

    # TODO 将来依据图片的大小，可以选择相应的水印图片
    def image_mark(self, source, target):
        fileName = source
        logoName = "/home/kulen/Documents/mark_logov3/logov3_60.png"
        logging.info(u'图片打水印:%s', fileName)
        im = Image.open(fileName)
        mark = Image.open(logoName)
        imWidth, imHeight = im.size
        if imWidth < 800 or imHeight < 400:
            logging.info(u'图片:%s 过小，不打水印', source)
            return
        markWidth, markHeight = mark.size
        print im.size
        print mark.size
        if im.mode != 'RGBA':  
            im = im.convert('RGBA')
        if mark.mode != 'RGBA':  
            mark = mark.convert('RGBA')
        layer = Image.new('RGBA', im.size, (0, 0, 0, 0))  
        x = 0
        y = 0
        # 处理nmr抓取的图片
        if imWidth <= 1200 and imHeight <= 1000:
            x = imWidth - markWidth - 30
            y = imHeight - markHeight - 25
            mark = Image.open("/home/kulen/Documents/mark_logov3/logov3_100.png")
            layer.paste(mark, (x, y))
        # 处理ChemDraw生成的图片
        elif imWidth > 1200 and imHeight > 1000:
            ynum = imHeight / 1000
            xnum = imWidth / 950
            print "xnum:%s ynum:%s" % (xnum, ynum)
            yunit = imHeight / ynum
            xunit = imWidth / xnum
            print "xunit:%s yunit:%s" % (xunit, yunit)
            i = 0;
            while i < ynum:
                j = 0
                print '-----------'
                while j < xnum:
                    y = i * yunit + (yunit / 2 - markHeight / 2)
                    x = j * xunit + (xunit / 2 - markWidth / 2)
                    print "X:%s Y:%s" % (x, y)
                    layer.paste(mark, (x, y))
                    j += 1
                i += 1
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
    # extract.list_file_dir(1, "/home/kulen/NmrMsdsETL/pdf_reader")
    # extract.get_data('2014-07-17')
    # extract.image_mark()
    # extract.mark_all_image()
    # extract.image_mark('/home/kulen/NmrMsdsETL/1.png', '/home/kulen/NmrMsdsETL/1m.png')
    extract.image_mark('/home/kulen/NmrMsdsETL/3.png', '/home/kulen/NmrMsdsETL/3m.png')
    logging.info(u'程序运行完成')
    # print os.listdir("/home/kulen/NmrMsdsETL/2014-07-17/000/000/014")
    
