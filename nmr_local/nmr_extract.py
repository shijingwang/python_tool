# -*- coding: utf-8 -*-
import logging
from tornado.options import define, options
import traceback
import win32gui
import win32con
import win32api
import os
import time
import threading
from dict_data import dict_conf

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

class Nmr(object):
    
    def __init__(self):
        self.Mhandle = win32gui.FindWindow("CSWFrame", None)
        if self.Mhandle == 0:
            self.startup_app() 
        self.initial_data()
        close_alert = self.__getattribute__('close_alert')
        cat = threading.Thread(target=close_alert)
        cat.start()

    
    def file_menu_command(self, command):
        """
        菜单操作
        返回弹出的打开或保存的对话框的句柄 dig_handle
        返回确定按钮的句柄 confBTN_handle
        """
        command_dict = { 
            "open": [1, u"打开"],
            "save_to_image": [8, u"另存为"],
        }
        cmd_ID = win32gui.GetMenuItemID(self.file_menu, command_dict[command][0])
        win32gui.PostMessage(self.Mhandle, win32con.WM_COMMAND, cmd_ID, 0)
        for i in range(10):
            if win32gui.FindWindow(None, command_dict[command][1]): 
                break  # 如果找到了打开或者另存为的对话框，就跳出循环
            else:
                win32api.Sleep(200)  # 利用这个函数等待200ms，就不需要再额外导入time模块了
        dig_handle = win32gui.FindWindow(None, command_dict[command][1])
        confBTN_handle = win32gui.FindWindowEx(dig_handle, 0, "Button", None)
        logging.info(u"dialogue_handle:%x button_handle:%x", dig_handle, confBTN_handle)
        return dig_handle, confBTN_handle

    def structure_menu_command(self, command):
        command_dict = { 
            "1H": [21, u"第一种核磁图"],
            "13C": [22, u"第二种核磁图"],
        }
        cmd_ID = win32gui.GetMenuItemID(self.structure_menu, command_dict[command][0])
        logging.info("NMR Handle:%x", cmd_ID)
        win32gui.PostMessage(self.Mhandle, win32con.WM_COMMAND, cmd_ID, 0)
    
    def open_mol(self, molfile):
        """打开Mol文件"""
        Mhandle, confirmBTN_handle = self.file_menu_command('open')
        handle = find_subHandle(Mhandle, [("ComboBoxEx32", 0), ("ComboBox", 0), ("Edit", 0)])
        TYPE_handle = find_subHandle(Mhandle, [("ComboBox", 1)])
        logging.info(u"打开按钮Handle:%x", handle)
        win32api.SendMessage(TYPE_handle, win32con.CB_SETCURSEL, 13, 0)
        win32api.SendMessage(TYPE_handle, win32con.CB_SETCURSEL, 13, 0)
        time.sleep(0.5)
        if win32api.SendMessage(handle, win32con.WM_SETTEXT, 0, os.path.abspath(molfile)) != 1:
            raise Exception("File opening path set failed")
        win32api.SendMessage(Mhandle, win32con.WM_COMMAND, 1, confirmBTN_handle)  
        time.sleep(0.8)
        MOL_handle = find_subHandle(self.Mhandle, [("MDIClient", 0), ("CSWDocument", 0)])
        
        win32gui.SetForegroundWindow(MOL_handle)  
        win32api.keybd_event(17, 0, 0, 0)  # Alt
        win32api.keybd_event(65, 0, 0, 0)  # F
        win32api.keybd_event(17, 0, win32con.KEYEVENTF_KEYUP, 0) 
        win32api.keybd_event(65, 0, win32con.KEYEVENTF_KEYUP, 0)
        time.sleep(0.5)
    
    
    def generate_1h_image(self, filepath):
        self.structure_menu_command("1H")
        # 等待软件生成图片
        time.sleep(1)
        self.save_to_image(filepath)
        pass
    
    def generate_13c_image(self, filepath):
        self.structure_menu_command("13C")
        time.sleep(1)
        self.save_to_image(filepath)
        pass
    
    def close_mol(self):
        Image_handle = find_subHandle(self.Mhandle, [("MDIClient", 0), ("CSWDocument", 0)])
        win32gui.SendMessage(Image_handle, win32con.WM_CLOSE, 0, 0)
    
    
    def save_to_image(self, filePath):  
        Mhandle, confirmBTN_handle = self.file_menu_command('save_to_image')  
        EDIT_handle = find_subHandle(Mhandle, [("ComboBoxEx32", 0), ("ComboBox", 0), ("Edit", 0)])  # 定位保存地址句柄
        TYPE_handle = find_subHandle(Mhandle, [("ComboBox", 1)])
          
        win32api.SendMessage(TYPE_handle, win32con.CB_SETCURSEL, 16, 0)
        win32api.SendMessage(TYPE_handle, win32con.CB_SETCURSEL, 16, 0)
        time.sleep(0.4)
        if win32api.SendMessage(EDIT_handle, win32con.WM_SETTEXT, 0, os.path.abspath(filePath)) != 1:
            raise Exception("Set file opening path failed")
        time.sleep(0.2)
        win32api.SendMessage(Mhandle, win32con.WM_COMMAND, 1, confirmBTN_handle)
        time.sleep(0.2)
        # 下面的方法会产生阻塞，所以需要开启新线程关闭窗口
        Image_handle = find_subHandle(self.Mhandle, [("MDIClient", 0), ("CSWDocument", 1)])
        win32gui.SendMessage(Image_handle, win32con.WM_CLOSE, 0, 0)
        return
    
    def close_alert(self):
        logging.info(u"启动关闭窗口进程")
        while True:
            Alert_handle = win32gui.FindWindow("#32770", u"ChemBioDraw Ultra")
            if Alert_handle==0:
                time.sleep(0.5)    
                continue
            logging.info(u"执行关闭窗口操作")
            Btn_handle = find_subHandle(Alert_handle, [("Button", 1)])
            logging.info("Alert_handle:%x  Btn_handle:%x", Alert_handle, Btn_handle)
            # win32gui.PostMessage(Alert_handle, win32con.WM_COMMAND, Btn_handle, 0)
            win32gui.SetForegroundWindow(Alert_handle)  
            
            win32api.keybd_event(9, 0, 0, 0)  # 右箭头
            win32api.keybd_event(9, 0, win32con.KEYEVENTF_KEYUP, 0) 
            time.sleep(0.2)
            win32api.keybd_event(13, 0, 0, 0)  # 回车
            win32api.keybd_event(13, 0, win32con.KEYEVENTF_KEYUP, 0)
            time.sleep(0.2)
        '''
        (left, top, right, bottom) = win32gui.GetWindowRect(Btn_handle)
        win32api.SetCursorPos((left + (right - left) / 2, top + (bottom - top) / 2)) 
        time.sleep(0.5)
        win32api.mouse_event(win32con.MOUSEEVENTF_LEFTDOWN, 0, 0) 
        time.sleep(0.1)
        win32api.mouse_event(win32con.MOUSEEVENTF_LEFTUP, 0, 0)
        time.sleep(0.1)
        '''
    
    def find_stop(self):
        close_handle = win32gui.FindWindow("#32770", u"ChemBioDraw Ultra 12.0")
        if close_handle == 0:
            return
        btn_handle = find_subHandle(close_handle, [("DirectUIHWND", 0), ("CtrlNotifySink", 9), ("Button", 0)])
        logging.info("close_handle:%x btn_handle:%x", close_handle, btn_handle)
        win32api.SendMessage(close_handle, win32con.WM_COMMAND, 1, btn_handle)
        time.sleep(1)
        self.startup_app()
    
    def startup_app(self):
        self.Mhandle = win32gui.FindWindow("CSWFrame", None)
        if self.Mhandle != 0:
            return
        win32api.ShellExecute(0, 'open', u'"%s"' % dict_conf.chemdraw_app, '','',1)
        time.sleep(10)
        self.initial_data()
    
    def initial_data(self):
        self.Mhandle = win32gui.FindWindow("CSWFrame", None)
        Image_handle = find_subHandle(self.Mhandle, [("MDIClient", 0), ("CSWDocument", 0)])
        logging.info("StartupEditHandle:%x", Image_handle)
        win32gui.SendMessage(Image_handle, win32con.WM_CLOSE, 0, 0)
        self.totalmenu = win32gui.GetMenu(self.Mhandle)
        self.file_menu = win32gui.GetSubMenu(self.totalmenu, 0)
        self.structure_menu = win32gui.GetSubMenu(self.totalmenu, 4)
        logging.info(u"Handle:%x menu:%x struc:%x", self.Mhandle, self.file_menu, self.structure_menu)

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="D:/log/nmr.log", help="NSQ topic")
    define("func_name", default="spider_apple")
    options.parse_command_line()
    logfile = options.logfile
    fmt = '%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s'
    handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=100 * 1024 * 1024, backupCount=10)  # 实例化handler
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt)  # 实例化formatter
    handler.setFormatter(formatter)  # 为handler添加formatter
    logging.getLogger('').addHandler(handler)
    logging.info(u'写入的日志文件为:%s', logfile)
    nmr = Nmr()
    #nmr.find_stop()
    #nmr.startup_app()
    
    nmr.open_mol("D:\\molfile\\1694271.mol")
    nmr.generate_1h_image("C:\\Users\\Administrator\\Desktop\\cp_1h.png")
    '''
    time.sleep(1)
    nmr.generate_13c_image("C:\\Users\\Administrator\\Desktop\\cp_13c.png")
    time.sleep(1)
    nmr.close_mol()
    time.sleep(1)
    '''
    logging.info(u'程序运行完成')
