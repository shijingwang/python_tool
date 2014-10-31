# -*- coding: utf-8 -*-
import json, base64
import logging
import os, sys, time
from PIL import Image
from tornado.options import define, options
import traceback
try:
    import python_tool
except ImportError:
    fp = os.path.abspath(__file__)
    sys.path.append(fp[0:fp.rfind('python_tool') + 11])
from common.con_util import ConUtil
import dict_conf
import CK
from nmr_local.nmr_extract import Nmr

class NmrPicWorker(object):
    
    def __init__(self):
        self.redis_server = ConUtil.connect_redis(dict_conf.TRANSFER_REDIS_SERVER)
        self.nmr = Nmr()

    def nmr_create_task(self):
        msg = self.redis_server.rpop(CK.R_NMR_CREATE)
        if not msg:
            return
        msg_j = json.loads(msg)
        logging.info("处理mol_id:%s cas_no:%s 生成NMR图片的请求", msg_j['mol_id'], msg_j['cas_no'])
        
        # 保存mol文件
        mol_file_path = "%s/%s.mol" % (dict_conf.chemdraw_work_dir, msg_j['mol_id'])
        f = file(mol_file_path, 'w')
        f.write(msg_j['mol'])
        f.close()
        self.generate_pic(msg_j['mol_id'], msg_j['cas_no'], mol_file_path)
        
    
    def nmr_create_task_thread(self):
        logging.info(u'启动NMR图片生成线程')
        while True:
            try:
                size = self.redis_server.llen(CK.R_NMR_CREATE)
                logging.info(u'NMRCreate队列中的数据大小:%s', size)
                if size == 0:
                    time.sleep(1)
                    continue
                self.nmr_create_task()
            except Exception, e:
                logging.error(u"NMRCreate队列中的数据处理出错,%s", e)
                logging.error(traceback.format_exc())
    
    def generate_pic(self, mol_id, cas_no, mol_file_path):
        
        # 原始何存的图片路径
        pic_1h = "%s/%s-1h.png" % (dict_conf.chemdraw_work_dir, mol_id)
        pic_13c = "%s/%s-13c.png" % (dict_conf.chemdraw_work_dir, mol_id)
        
        # 清洗后的图片路径
        c_pic_1h = "%s/ext_%s-1h.png" % (dict_conf.chemdraw_work_dir, mol_id)
        c_pic_13c = "%s/ext_%s-13c.png" % (dict_conf.chemdraw_work_dir, mol_id)
        
        # mark后的图片路径
        m_pic_1h = "%s/mark_%s-1h.png" % (dict_conf.chemdraw_work_dir, mol_id)
        m_pic_13c = "%s/mark_%s-13c.png" % (dict_conf.chemdraw_work_dir, mol_id)
            
        try:
            mol_file_path = mol_file_path.replace("/", "\\")
            pic_1h = pic_1h.replace("/", "\\")
            pic_13c = pic_13c.replace("/", "\\")
            self.delete_file(pic_1h)
            self.delete_file(pic_13c)
            self.nmr.open_mol(mol_file_path)
            self.nmr.generate_1h_image(pic_1h)
            time.sleep(1)
            self.nmr.generate_13c_image(pic_13c)
            time.sleep(1)
            self.nmr.close_mol()
            time.sleep(1)
            self.clean_img(pic_1h)
            self.clean_img(pic_13c)
            
            self.image_mark(c_pic_1h, m_pic_1h)
            self.image_mark(c_pic_13c, m_pic_13c)
            
            msg = {'mol_id':mol_id}
            msg['cas'] = cas_no
            pic_dict = {'1h':m_pic_1h, '13c':m_pic_13c}
            for key in pic_dict:
                try:
                    if not os.path.exists(pic_dict[key]):
                        continue
                    img_reader = open(pic_dict[key], 'rb')
                    v_img = img_reader.read()
                    img_reader.close()
                    im = Image.open(pic_dict[key])
                    width, height = im.size
                    e_img = base64.encodestring(v_img)
                    pic_info = {}
                    pic_info['img'] = e_img
                    pic_info['width'] = width
                    pic_info['height'] = height
                    msg[key] = pic_info
                except Exception, e:
                    logging.error(u'生成图片信息出错:%s', e)
            msg_j = json.dumps(msg)
            counter = 0
            while True:
                counter += 1
                try:
                    self.redis_server.lpush(CK.R_NMR_IMPORT, msg_j)
                    break
                except Exception, e:
                    logging.info(u'向Redis提交信息出错')
                    time.sleep(1)
                if counter >= 15:
                    break
        except Exception, e:
            self.nmr.find_stop()
            self.nmr.startup_app()
            logging.error(u'生成mol_id:%s 核磁数据出错', mol_id, e);
            logging.error(traceback.format_exc())
        finally:
            self.delete_file(mol_file_path) 
            self.delete_file(pic_1h)
            self.delete_file(pic_13c)
            self.delete_file(c_pic_1h)
            self.delete_file(c_pic_13c)     
            self.delete_file(m_pic_1h)  
            self.delete_file(m_pic_13c)

    def delete_file(self, fp):
        try:
            if os.path.exists(fp):
                os.remove(fp)
        except Exception:
            pass
    
    # 对生成的NMR图片进行相应的清洗工作
    def clean_img(self, img_file):
        command = "%s %s" % (dict_conf.nmr_img_clean, img_file)
        logging.info(u'执行指令:%s', command)
        os.system(command)
    
    # TODO 将来依据图片的大小，可以选择相应的水印图片
    def image_mark(self, source, target):
        check_dir = target[:target.rfind('/')]
        # print check_dir
        if not os.path.exists(check_dir):
            os.makedirs(check_dir)
        fileName = source
        logoName = dict_conf.MARK_LOGO_IMG
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
        if imWidth > 1200 and imHeight > 1000:
            ynum = imHeight / 600
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
        
        if imWidth > 880:
            nHeight = (imHeight * 880) / imWidth
            layer = layer.resize((880, nHeight))
            im = im.resize((880, nHeight), Image.ANTIALIAS)

        Image.composite(layer, im, layer).save(target, quality=80)
        logging.info(u'图片完成打水印:%s', fileName)
    
    
if __name__ == '__main__':

    reload(sys)
    sys.setdefaultencoding('utf-8')
    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="c:\\nmr_worker.log", help="NSQ topic")
    options.parse_command_line()
    logfile = options.logfile
    fmt = '%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s'
    handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=100 * 1024 * 1024, backupCount=10)  # 实例化handler
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt)  # 实例化formatter
    handler.setFormatter(formatter)  # 为handler添加formatter
    logging.getLogger('').addHandler(handler)
    logging.info(u'写入的日志文件为:%s', logfile)
    npw = NmrPicWorker()
    # npw.nmr_create_task()
    npw.nmr_create_task_thread()
    # npw.resize_pic()
    logging.info(u'程序运行完成')
