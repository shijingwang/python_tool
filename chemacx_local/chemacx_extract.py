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

class ChemacxExtract(object):
    
    def __init__(self):
        self.Mhandle = win32gui.FindWindow("CSWFrame", None)
        if self.Mhandle == 0:
            self.startup_app() 
        self.initial_data()

    
    def file_menu_command(self, command):
        """
        菜单操作
        返回弹出的打开或保存的对话框的句柄 dig_handle
        返回确定按钮的句柄 confBTN_handle
        """
        command_dict = { 
            "new": [0, u"新建文档"],
            "save_to_mol": [8, u"另存为"],
        }
        cmd_ID = win32gui.GetMenuItemID(self.file_menu, command_dict[command][0])
        win32gui.PostMessage(self.Mhandle, win32con.WM_COMMAND, cmd_ID, 0)
        for i in range(10):
            if win32gui.FindWindow(None, command_dict[command][1]): 
                break  # 如果找到了打开或者另存为的对话框，就跳出循环
            else:
                time.sleep(1)
        dig_handle = win32gui.FindWindow(None, command_dict[command][1])
        confBTN_handle = win32gui.FindWindowEx(dig_handle, 0, "Button", None)
        logging.info(u"dialogue_handle:%x button_handle:%x", dig_handle, confBTN_handle)
        return dig_handle, confBTN_handle

    def online_menu_command(self, command):
        command_dict = { 
            "EntryAcxNumber": [1, u"Entry AcxNumber"],
        }
        cmd_ID = win32gui.GetMenuItemID(self.online_menu, command_dict[command][0])
        logging.info("NMR Handle:%x", cmd_ID)
        win32gui.PostMessage(self.Mhandle, win32con.WM_COMMAND, cmd_ID, 0)
        
    def entry_acx_number(self, acx_number):
        self.online_menu_command("EntryAcxNumber")
        time.sleep(0.1)
        entry_window = win32gui.FindWindow("#32770",u"Find Structure from ACX Number")
        edit_handle=find_subHandle(entry_window, [("Edit", 0)])
        confirm_handle=find_subHandle(entry_window, [("Button", 0)])
        if win32api.SendMessage(edit_handle, win32con.WM_SETTEXT, 0, acx_number) != 1:
            raise Exception("Set Entry Number failed")
        time.sleep(0.1)
        win32api.SendMessage(entry_window, win32con.WM_COMMAND, 1, confirm_handle)
        time.sleep(0.1)
    
    def new_blank_document(self):
        self.file_menu_command('new')
        time.sleep(0.1)
    
    def close_document(self):
        Image_handle = find_subHandle(self.Mhandle, [("MDIClient", 0), ("CSWDocument", 0)])
        if Image_handle == 0:
            return
        win32gui.SendMessage(Image_handle, win32con.WM_CLOSE, 0, 0)
        time.sleep(0.1)
    
    def save_to_mol(self, filePath):  
        Mhandle, confirmBTN_handle = self.file_menu_command('save_to_mol')  
        EDIT_handle = find_subHandle(Mhandle, [("ComboBoxEx32", 0), ("ComboBox", 0), ("Edit", 0)])  # 定位保存地址句柄
        TYPE_handle = find_subHandle(Mhandle, [("ComboBox", 1)])
        
        counter = 0
        win32api.SendMessage(TYPE_handle, win32con.CB_SETCURSEL, 12, 0)
        while True:
            counter += 1
            time.sleep(0.1)
            if win32api.SendMessage(EDIT_handle, win32con.WM_SETTEXT, 0, os.path.abspath(filePath)) != 1:
                continue
            else:
                break
            if counter>=200:
                raise Exception("Save mol file error")
        time.sleep(0.1)
        win32api.SendMessage(Mhandle, win32con.WM_COMMAND, 1, confirmBTN_handle)
        time.sleep(1)
    
    def close_alert(self):
        logging.info(u"启动关闭窗口进程")
        while True:
            Alert_handle = win32gui.FindWindow("#32770", u"ChemBioDraw Ultra")
            if Alert_handle == 0:
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
        win32api.ShellExecute(0, 'open', u'"C:\\Program Files (x86)\\CambridgeSoft\\ChemOffice2010\\ChemDraw\\ChemDraw.exe"', '', '', 1)
        time.sleep(6)
        self.initial_data()
    
    def initial_data(self):
        self.Mhandle = win32gui.FindWindow("CSWFrame", None)
        self.totalmenu = win32gui.GetMenu(self.Mhandle)
        self.file_menu = win32gui.GetSubMenu(self.totalmenu, 0)
        self.online_menu = win32gui.GetSubMenu(self.totalmenu, 8)
        self.close_document()
        self.new_blank_document()
        logging.info(u"Handle:%x menu:%x struc:%x", self.Mhandle, self.file_menu, self.online_menu)

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
    chemacxExtract = ChemacxExtract()
    #chemacxExtract.online_menu_command("EntryAcxNumber")
    chemacxExtract.entry_acx_number('X1029414-7')
    time.sleep(6)
    chemacxExtract.save_to_mol('C://Users//Administrator//Desktop//1.mol')
    logging.info(u'程序运行完成')
