# -*- coding: utf-8 -*-
from tornado.options import define, options
import logging
import traceback
import os
import Image, ImageDraw, ImageFont
import math

from common.con_util import ConUtil
import settings

class Extract(object):
    
    def __init__(self):
        self.molbase_db = ConUtil.connect_mysql(settings.MYSQL_MOLBASE)
        self.file_list = []
        
    # stype->source type
    def get_data(self, stype, day):
        sql = "insert into search_nmr (type,cas,path,width,height) values (%s,'%s','%s',%s,%s) on duplicate key update path='%s',width=%s,height=%s"
        counter = 0
        for f in self.file_list:
            counter += 1
            try:
                im = Image.open(f)
                fp = f[f.find('/' + day):]
                width, height = im.size
                if stype == 'nmrdb':
                    ftype = 3
                    cas = fp[fp.rfind('/') + 1:fp.rfind('.')]
                else:
                    ftype = 1 if '1h' in fp else 2
                    cas = fp[fp.rfind('/') + 1:fp.rfind('-')]
                logging.info(u'处理cas:%s', cas)
                isql = sql % (ftype, cas, fp, width, height, fp, width, height)
                self.molbase_db.insert(isql)
            except Exception, e:
                logging.error(traceback.format_exc())
        
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
        check_dir = target[:target.rfind('/')]
        # print check_dir
        if not os.path.exists(check_dir):
            os.makedirs(check_dir)
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
        logging.info("图片大小:%s", im.size)
        # print mark.size
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
            # print "xnum:%s ynum:%s" % (xnum, ynum)
            yunit = imHeight / ynum
            xunit = imWidth / xnum
            # print "xunit:%s yunit:%s" % (xunit, yunit)
            i = 0;
            while i < ynum:
                j = 0
                # print '-----------'
                while j < xnum:
                    y = i * yunit + (yunit / 2 - markHeight / 2)
                    x = j * xunit + (xunit / 2 - markWidth / 2)
                    # print "X:%s Y:%s" % (x, y)
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
    
    def extract_nmrdb_data(self):
        days = ["2014-07-07", "2014-07-08", "2014-07-09", "2014-07-10", "2014-07-11", "2014-07-12", "2014-07-13", "2014-07-14", "2014-07-15", "2014-07-16"]
        for day in days:
            self.file_list = []
            self.list_file_dir(1, settings.NMR_DB_FILE_PATH_T + day)
            self.get_data('nmrdb', day)
            
    def mark_nmrdb_data(self):
        days = ["2014-07-07", "2014-07-08", "2014-07-09", "2014-07-10", "2014-07-11", "2014-07-12", "2014-07-13", "2014-07-14", "2014-07-15", "2014-07-16"]
        for day in days:
            self.file_list = []
            self.list_file_dir(1, settings.NMR_DB_FILE_PATH_S + day)
            for f in self.file_list:
                target = settings.NMR_DB_FILE_PATH_T + f[f.find(settings.NMR_DB_FILE_PATH_S) + len(settings.NMR_DB_FILE_PATH_S):]
                self.image_mark(f, target)
            
    
    def extract_nmrchem_data(self):
        days = ["2014-07-17", "2014-07-18", "2014-07-19", "2014-07-20", "2014-07-21"]
        for day in days:
            self.file_list = []
            self.list_file_dir(1, settings.NMR_CHEM_FILE_PATH_T + day)
            self.get_data('nmrchem', day)
    
    def mark_nmrchem_data(self):
        days = ["2014-07-19", "2014-07-20", "2014-07-21"]
        for day in days:
            self.file_list = []
            self.list_file_dir(1, settings.NMR_CHEM_FILE_PATH_S + day)
            for f in self.file_list:
                target = settings.NMR_CHEM_FILE_PATH_T + f[f.find(settings.NMR_CHEM_FILE_PATH_S) + len(settings.NMR_CHEM_FILE_PATH_S):]
                try:
                    self.image_mark(f, target)
                except Exception, e:
                    logging.error(traceback.format_exc())

if __name__ == '__main__':

    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="/home/kulen/log/run.log", help="NSQ topic")
    define("func_name", default="spider_apple")
    options.parse_command_line()
    logfile = options.logfile
    func_name = options.func_name
    logging.info(u'写入的日志文件为:%s', logfile)
    
    extract = Extract()
    # extract.mark_all_image()
    # extract.image_mark('/home/kulen/NmrMsdsETL/1.png', '/home/kulen/NmrMsdsETL/1m.png')
    # extract.image_mark('/home/kulen/NmrMsdsETL/3.png', '/home/kulen/NmrMsdsETL/3m.png')
    # extract.list_file_dir(1, '/home/kulen/NmrMsdsETL/nmrdb_file_p/2014-07-07')
    # extract.get_data('nmrdb', '2014-07-07')
    # extract.extract_nmrdb_data()
    extract.extract_nmrchem_data()
    logging.info(u'程序运行完成')
    # print os.listdir("/home/kulen/NmrMsdsETL/2014-07-17/000/000/014")
    
