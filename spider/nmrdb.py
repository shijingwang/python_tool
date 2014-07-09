# -*- coding: utf-8 -*-
import logging
from tornado.options import define, options
import win32gui, win32con, win32api, winGuiAuto 
from webscraping import adt, common, download, xpath
from down_util2 import DownUtil2
import settings
import os
import cPAMIE
import time
import datetime
import sys
from bs4 import BeautifulSoup

DELAY = 5
TIME_OUT = 30
MAX_TIME_OUT = 45
HIDE_IE_WINDOW = '--hideie' in sys.argv

class Nmrdb(object):
    
    def __init__(self):
        pass
    
    def elapsed(self, start):
        seconds = time.time() - start
        return seconds

    def click_popup_window_OK_button(self, title=''):
        '''用PAMIE实现点击网页中的弹出窗口的按钮
        '''
        found_OK_button = False
        hwnd = winGuiAuto.findTopWindows(title)
        if hwnd:
            control_list = winGuiAuto.dumpWindow(hwnd[0])
            for control_item in control_list:
                if control_item[1] == 'OK' or control_item[1] == u'确定'.encode('gbk'):
                    found_OK_button = True
                    winGuiAuto.clickButton(control_item[0])
                
    def startup_ie(self):    
        ''' 启动IE
        '''
        try_num = 0
        start = time.time()
        while True:
            try:
                ie = cPAMIE.PAMIE(url='', timeOut=MAX_TIME_OUT)
                if HIDE_IE_WINDOW:
                    ie._ie.Visible = 0 
                if winGuiAuto.findTopWindows(u"iexplore.exe - 应用程序错误".encode('gbk')):
                    self.click_popup_window_OK_button(title=u"iexplore.exe - 应用程序错误".encode('gbk'))
                    time.sleep(2)
                    continue
                return ie
            except Exception, e:
                print e
            if self.elapsed(start) > 30:
                break
            try_num += 1
            if try_num > 5:
                break
            time.sleep(2)
            
        return None
    
    def close_alter_window(self, elapsed_seconds=1):
        '''点击安全警告窗口中的“运行”按钮, 尝试3次失败后关闭该窗口
        '''
        try_num = 0
        start = time.time()
        while True:
            hwnd = winGuiAuto.findTopWindows(u"安全警告".encode('gbk'))
            if hwnd:
                try:
                    (left, top, right, bottom) = win32gui.GetWindowRect(hwnd[0])     
                    x = left + 460
                    y = top + 255
                    win32api.SetCursorPos((x, y))
                    win32api.ClipCursor((x, y, x, y))
                    win32api.mouse_event(win32con.MOUSEEVENTF_LEFTDOWN, 0, 0, 0, 0)
                    win32api.mouse_event(win32con.MOUSEEVENTF_LEFTUP, 0, 0, 0, 0)
                    win32api.ClipCursor((0, 0, 0, 0))
                except Exception, e:
                    print e
                    win32api.ClipCursor((0, 0, 0, 0))
                    try_num += 1
                    if try_num > 3:
                        win32api.SendMessage(hwnd[0], win32con.WM_CLOSE, 0, 0)
                        break
                    time.sleep(3)
                    continue
            if elapsed_seconds == 0:
                break
            if self.elapsed(start) > 10:
                break
            time.sleep(1)   

    def quit_ie(self, ie):
        '''关闭打开的IE浏览器
        '''
        start = time.time()
        while True:
            time.sleep(2)
            try:
                hwnd = winGuiAuto.findTopWindows("Windows Internet Explorer")
                if hwnd:
                    win32api.SendMessage(hwnd[0], win32con.WM_CLOSE, 0, 0)  # 关闭IE窗口
                else:
                    break
            except Exception, e:
                print e
            if self.elapsed(start) > 60:
                common.logger.error('Quit IE failed.')
                break
        ie = None
        return ie    
    

    def download_nmr(self, cas):
        start_time = int(time.time())
        logging.info(u"抓取CAS:%s核磁共振数据", cas)
        try:
            ie = self.startup_ie()
            hwd_ie = winGuiAuto.findTopWindows("Windows Internet Explorer")
            url = settings.APP_PATH + 'predictor.htm'
            ie.navigate(url)
            hwnd = winGuiAuto.findTopWindows(u"安全警告".encode('gbk'))
            if hwnd:
                self.close_alter_window(elapsed_seconds=0)
            get_data_link = ie.elementFind('a', 'name', 'getresultdata')
            ie.elementClick(get_data_link)
            counter = 0
            while True:
                counter += 1
                text_tableformat_value = ie.textAreaGetValue('tableformat', 'value')
                if text_tableformat_value:
                    btn_get_pdf = ie.elementFind('input', 'value', 'Get PDF')
                    ie.elementClick(btn_get_pdf)
                    if HIDE_IE_WINDOW:
                        ie._ie.Visible = 0
                    time.sleep(1)
                    break
                elif counter>9:
                    raise Exception(u'CAS号无核磁数据', 555)
                    break
                else:
                    time.sleep(1)
            html = ie.outerHTML()
            if 'action=/cheminfo/servlet/org.cheminfo.hook.appli.HookServlet' in html or 'action=http://www.nmrdb.com/cheminfo/servlet/org.cheminfo.hook.appli.HookServlet' in html:
                soup = BeautifulSoup(html)
                para_molfile = xpath.get(html, '//TEXTAREA[@name=molfile]').strip()
                # para_molfile = soup.find('TEXTAREA', {'name':'molfile'})
                # print '-------------------------molfile'
                # print para_molfile
                para_ethylvinylether = xpath.get(html, '//TEXTAREA[@name=ethylvinylether]').strip()
                para_assignment = xpath.get(html, '//TEXTAREA[@name=assignment]').strip()
                # para_assignment = soup.find('TEXTAREA', {'name':'assignment'})
                # print '-------------------------assignment'
                # print para_assignment
                para_tableformat = xpath.get(html, '//TEXTAREA[@name=tableformat]').strip()
                # para_tableformat = soup.find('TEXTAREA', {'name':'tableformat'})
                # print '-------------------------tableformat'
                # print para_tableformat
                para_url = xpath.get(html, '//input[@name=url]/@value').replace('&amp;', '&').strip()
                para_url = soup.find('input', {'name':'url'})['value']
                # print '-------------------------url'
                # print para_url
                para_xmlString = xpath.get(html, '//INPUT[@name=xmlString]/@value').strip()
                para_xmlString = soup.find("input", {'name':'xmlString'})['value']
                # print '-------------------------xmlString'
                # print para_xmlString
                
                if not para_xmlString:
                    para_xmlString = common.regex_get(html, r'<INPUT value="([^"]+)"\s*type=hidden name=xmlString>', normalized=False)
                if not para_xmlString:
                    para_xmlString = common.regex_get(html, r"<INPUT value='([^']+)'\s*type=hidden name=xmlString>", normalized=False)
                # para_xmlString = common.unescape(para_xmlString).replace('&quot;', '"')
                para_resolution = xpath.get(html, '//input[@name=resolution]/@value').strip()
                para_rotate = xpath.get(html, '//input[@name=rotate]/@value').strip()
                get_pdf_url = 'http://www.nmrdb.org/cheminfo/servlet/org.cheminfo.hook.appli.HookServlet'
                post_data2 = {}
                post_data2['molfile'] = para_molfile
                # post_data2['ethylvinylether'] = para_ethylvinylether
                post_data2['assignment'] = para_assignment
                post_data2['tableformat'] = para_tableformat
                post_data2['width'] = '800'
                post_data2['height'] = '600'
                post_data2['url'] = para_url
                post_data2['xmlString'] = para_xmlString
                post_data2['resolution'] = para_resolution
                post_data2['rotate'] = para_rotate
                post_data2['options'] = ''
                post_data2['format'] = 'pdf'
                days = datetime.datetime.now().strftime('%Y-%m-%d')
                save_path = settings.SAVE_PATH + days + '/'
                if not os.path.exists(save_path):
                    os.mkdir(save_path)
                du = DownUtil2()
                du.downfile(get_pdf_url, post_data2, save_path , cas + '.pdf')
        except Exception, e:
            raise e
        finally:
            ie = self.quit_ie(ie)
        stop_time = int(time.time())
        logging.info(u'抓取CAS:%s核磁共振数据用时:%s秒', cas, stop_time - start_time)
        

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
    nmrdb = Nmrdb()
    nmrdb.download_nmr('1')
    
