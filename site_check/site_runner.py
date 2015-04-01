# -*- coding: utf-8 -*-

import win32gui
import win32ui
import win32con
import win32api
import time
import win32clipboard
import xml.sax
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import datetime
import sc_setting
import traceback
import sys
import os
from result_parse import ResultParse

import logging
from tornado.options import define, options

from ctypes import *

def find_idxSubHandle(pHandle, winClass, index=0):
    """
    已知子窗口的窗体类名
    寻找第index号个同类型的兄弟窗口
    """
    assert type(index) == int and index >= 0
    handle = win32gui.FindWindowEx(pHandle, 0, winClass, None)
    while index > 0:
        handle = win32gui.FindWindowEx(pHandle, handle, winClass, None)
        index -= 1
    return handle


def find_subHandle(pHandle, winClassList):
    """
    递归寻找子窗口的句柄
    pHandle是祖父窗口的句柄
    winClassList是各个子窗口的class列表，父辈的list-index小于子辈
    """
    assert type(winClassList) == list
    if len(winClassList) == 1:
        return find_idxSubHandle(pHandle, winClassList[0][0], winClassList[0][1])
    else:
        pHandle = find_idxSubHandle(pHandle, winClassList[0][0], winClassList[0][1])
        return find_subHandle(pHandle, winClassList[1:])

class SiteRunner(object):
    
    def __init__(self):
        # signal.signal(signal.SIGALRM, self.__getattribute__("handler"))
        pass
    
    def start_check(self):
        width = win32api.GetSystemMetrics(win32con.SM_CXSCREEN)
        height = win32api.GetSystemMetrics(win32con.SM_CYSCREEN)
        logging.info(u'屏幕分辨率:%s x %s', width, height)
        # main_handle
        while True:
            self.m_handle = win32gui.FindWindow('WindowsForms10.Window.8.app.0.33c0d9d', None)
            if self.m_handle == 0:
                logging.info(u"启动测试程序");
                win32api.ShellExecute(0, 'open', sc_setting.nunit_app_path, '', '', 1)
                time.sleep(10)
            else:
                break
        logging.info(u'程序主窗口句柄:%x', self.m_handle);
        win32gui.SetForegroundWindow(self.m_handle)  
        # mainframe_handle
        # self.mf_handle = find_subHandle(self.m_handle, [('WindowsForms10.Window.8.app.0.33c0d9d', 0)])
        # logging.info('Main Frame句柄:%x', self.mf_handle);
        # containframe_handle
        # self.cf_handle = find_subHandle(self.m_handle, [('WindowsForms10.Window.8.app.0.33c0d9d', 0), ('WindowsForms10.Window.8.app.0.33c0d9d', 0)])
        # logging.info('Contain Frame句柄:%x', self.cf_handle);
        
        # self.run_handle = find_subHandle(self.m_handle, [('WindowsForms10.Window.8.app.0.33c0d9d', 0), ('WindowsForms10.Window.8.app.0.33c0d9d', 0), ('WindowsForms10.BUTTON.app.0.33c0d9d', 0)]);
        # logging.info('Run Button句柄:%x', self.run_handle);
        
        # self.stop_handle = find_subHandle(self.m_handle, [('WindowsForms10.Window.8.app.0.33c0d9d', 0), ('WindowsForms10.Window.8.app.0.33c0d9d', 0), ('WindowsForms10.BUTTON.app.0.33c0d9d', 1)]);
        # logging.info('Stop Button句柄:%x', self.stop_handle);
        
        win32api.keybd_event(116, 0, 0, 0)
        win32api.keybd_event(116, 0, win32con.KEYEVENTF_KEYUP, 0)
        time.sleep(0.5)
        
        # win32gui.PostMessage(self.run_handle, win32con.WM_LBUTTONDOWN, win32con.MK_LBUTTON, 0)
        # win32gui.PostMessage(self.run_handle, win32con.WM_LBUTTONUP, win32con.MK_LBUTTON, 0)
        
        # win32gui.PostMessage(self.run_handle, win32con.WM_KEYDOWN, win32con.VK_RETURN, 0)
        # win32gui.PostMessage(self.run_handle, win32con.WM_KEYUP, win32con.VK_RETURN, 0)
        # self.run_handle.SendMessage(win32con.BM_CLICK, 0, -1)
        pass;
    
    def parse_xml(self):
        parse_pass = False
        try:
            parser = xml.sax.make_parser()
            parser.setFeature(xml.sax.handler.feature_namespaces, 0)
            result_parse = ResultParse()
            parser.setContentHandler(result_parse)
            parser.parse(sc_setting.test_result_path)
            test_result = []
            for key in result_parse.test_result:
                logging.info(u"测试节点名称:%s", key)
                keys = result_parse.test_result[key].keys() 
                keys.sort()
                for key1 in keys:
                    logging.info(u'测试组名称:%s', key1)
                    fail_result = []
                    for value in result_parse.test_result[key][key1]:
                        if value['success'] != 'True':
                            logging.info(u"name:%s result:%s msg:%s", value['name'], value['success'], value['msg'])
                            fail_result.append(u'    测试用例:%s 未通过测试' % value['name'])
                    if len(fail_result) > 0:
                        test_result.append(key)
                        test_result.append('  ' + key1)
                        test_result.extend(fail_result)
            parse_pass = True
        except Exception, e:
            logging.error(u'解析测试结果出错')
            logging.error(traceback.format_exc())
            
        # 测试失败
        if parse_pass:
            if len(test_result) > 0:
                # 发送QQ通知
                msg = u'Alert!!!,网站测试未通过,详情见邮件\n'
                for r in test_result:
                    msg = msg + r + '\n'
                msg = msg + u'测试用时:%s\n' % result_parse.total_spend_time
                msg = msg + u'详细内容见邮件附件'
                self.send_clipboard_msg(msg)
                self.send_qq_msg()
                msg = msg.replace('\n', '<br />')
                msg = msg.replace(' ', '&nbsp;')
                for email in sc_setting.error_notify_email:
                    self.send_mail(email, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' 网站测试未通过', msg)
            else:
                msg = u'测试用时:%s\n' % result_parse.total_spend_time
                for email in sc_setting.status_notify_email:
                    self.send_mail(email, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' 网站测试通过', msg)
        else:
            msg = u'Alert!!!,网站解析结果出错,详情见邮件或查看程序日志\n'
            self.send_clipboard_msg(msg)
            self.send_qq_msg()
            for email in sc_setting.status_notify_email:
                    self.send_mail(email, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' 网站测试结果解析出错', msg)
    
    def send_qq_msg(self):
        self.msg_handle = win32gui.FindWindow('ChatBox_PreviewWnd', None)
        logging.info('聊天窗口句柄:%x type:%s', self.msg_handle, type(self.msg_handle));
        win32gui.SetForegroundWindow(self.msg_handle)
        
        # 鼠标点击对话框
        # windll.user32.SetCursorPos(200,450)
        # time.sleep(0.5)
        # cur_pos = win32gui.GetCursorPos()
        # logging.info("鼠标座标为:%s", cur_pos)
        # win32api.mouse_event(win32con.MOUSEEVENTF_LEFTDOWN, 0, 0)
        # time.sleep(0.05)
        # win32api.mouse_event(win32con.MOUSEEVENTF_LEFTUP, 0, 0)
        
        # 粘贴剪切板的内容
        win32api.keybd_event(17, 0, 0, 0)
        win32api.keybd_event(86, 0, 0, 0)
        win32api.keybd_event(17, 0, win32con.KEYEVENTF_KEYUP, 0)
        win32api.keybd_event(86, 0, win32con.KEYEVENTF_KEYUP, 0)
        time.sleep(0.5)
        
        # 发送消息
        win32api.keybd_event(17, 0, 0, 0)
        win32api.keybd_event(13, 0, 0, 0)
        win32api.keybd_event(17, 0, win32con.KEYEVENTF_KEYUP, 0)
        win32api.keybd_event(13, 0, win32con.KEYEVENTF_KEYUP, 0)
        
        # win32api.keybd_event(67, 0, 0, 0)
        # win32api.keybd_event(67, 0, win32con.KEYEVENTF_KEYUP, 0)
    
    
    def send_clipboard_msg(self, msg):
        win32clipboard.OpenClipboard()
        win32clipboard.EmptyClipboard()
        win32clipboard.SetClipboardData(win32con.CF_UNICODETEXT, msg)
        win32clipboard.CloseClipboard()
    
    def send_mail(self, email, title, content):
        # 加邮件头
        logging.info(u"向用户:%s 发送邮件", email)
        for i in range(0, 20):
            try:
                msg = MIMEMultipart()
                txt1 = MIMEText(content, _subtype='html', _charset='UTF-8')
                # txt1.replace_header('Content-Transfer-Encoding', 'quoted-printable')  # 否则邮件原文看不懂，但并不影响读信
                msg.attach(txt1)
                # 带附件
                if os.path.exists(sc_setting.test_result_path):
                    att1 = MIMEText(open(sc_setting.test_result_path, 'rb').read(), 'base64', 'utf-8')
                    att1["Content-Type"] = 'application/octet-stream'
                    att1["Content-Disposition"] = 'attachment; filename="TestResult.xml"'  # 这里的filename可以任意写，写什么名字，邮件中显示什么名字
                    msg.attach(att1)
                msg['to'] = email
               
                msg['from'] = 'guoqiang.zhang@molbase.com'
                msg['subject'] = title
                # 发送邮件
                logging.info("start connect:%s", sc_setting.mail_send_server)
                server = smtplib.SMTP()
                logging.info("connect server")
                server.connect(sc_setting.mail_send_server)
                logging.info("connect success")
                server.login(sc_setting.mail_user, sc_setting.mail_password)
                logging.info("login success")
                server.sendmail(msg['from'], msg['to'], msg.as_string())
                server.quit()
                logging.info(u"第%s次邮件发送成功", i + 1)
                break
            except Exception, e:  
                logging.error(u"邮件发送失败,%s", e)
                logging.error(traceback.format_exc())
    
    def start_monitor(self):
        logging.info(u"启动网站检测应用")
        while True:
            logging.info(u"开始对网站进行相应的检测")
            try:
                if os.path.exists(sc_setting.test_result_path):
                    logging.info(u'删除测试结果文件:%s', sc_setting.test_result_path)
                    os.remove(sc_setting.test_result_path)
                self.start_check()
                # 判断程序是否运行完成
                counter = 0
                while True:
                    counter += 1
                    if os.path.exists(sc_setting.test_result_path):
                        break
                    elif counter >= 1200:
                        raise Exception('程序运行时间超过20分钟')
                        break
                    if counter % 30 == 0:
                        logging.info(u'正在对网站进行测试验证...')
                    time.sleep(1)
                self.parse_xml()
            except Exception, e:
                logging.error(u'检测网站程序运行出错')
                logging.error(traceback.format_exc())
                
            logging.info(u"完成对网站数据的检测")
            #break
            time.sleep(60*10)
    

if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')
    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="D:/log/site_check.log", help="NSQ topic")
    options.parse_command_line()
    logfile = options.logfile
    
    fmt = '%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s'
    handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=100 * 1024 * 1024, backupCount=10)  # ʵ��handler
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt)  # ʵ��formatter
    handler.setFormatter(formatter)  # Ϊhandler���formatter
    logging.getLogger('').addHandler(handler)
    logging.info(u'写入的日志文件为:%s', logfile)
   
    sr = SiteRunner()
    # sr.start_check()
    # sr.parse_xml()
    # sr.send_clipboard_msg()
    # sr.send_qq_msg()
    # sr.parse_xml()
    sr.start_monitor()
    # logging.info(os.path.exists(sc_setting.test_result_path))
    logging.info(u'程序运行完成')
