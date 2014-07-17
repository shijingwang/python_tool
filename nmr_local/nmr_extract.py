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

class Nmr(object):
    
    def __init__(self):
        self.Mhandle = win32gui.FindWindow("CSWFrame", None)
        self.totalmenu = win32gui.GetMenu(self.Mhandle)
        self.file_menu = win32gui.GetSubMenu(self.totalmenu, 0)
        self.structure_menu = win32gui.GetSubMenu(self.totalmenu, 4)
        logging.info(u"Handle:%x menu:%x struc:%x", self.Mhandle, self.file_menu, self.structure_menu)

    
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
        print "NMR Handle:%x" % cmd_ID
        win32gui.PostMessage(self.Mhandle, win32con.WM_COMMAND, cmd_ID, 0)
    
    def open_mol(self, molfile):
        """打开Mol文件"""
        Mhandle, confirmBTN_handle = self.file_menu_command('open')
        handle = find_subHandle(Mhandle, [("ComboBoxEx32", 0), ("ComboBox", 0), ("Edit", 0)])
        logging.info(u"打开按钮Handle:%x", handle)
        if win32api.SendMessage(handle, win32con.WM_SETTEXT, 0, os.path.abspath(molfile)) != 1:
            raise Exception("File opening path set failed")
        win32api.SendMessage(Mhandle, win32con.WM_COMMAND, 1, confirmBTN_handle)  
        time.sleep(0.5)
        MOL_handle = find_subHandle(self.Mhandle, [("MDIClient", 0), ("CSWDocument", 0)])
        
        win32gui.SetForegroundWindow(MOL_handle)  
        win32api.keybd_event(17, 0, 0, 0)  # Alt
        win32api.keybd_event(65, 0, 0, 0)  # F
        win32api.keybd_event(17, 0, win32con.KEYEVENTF_KEYUP, 0) 
        win32api.keybd_event(65, 0, win32con.KEYEVENTF_KEYUP, 0)
        time.sleep(0.5)
    
    
    def generate_1h_image(self):
        self.structure_menu_command("1H")
        # 等待软件生成图片
        time.sleep(1)
        self.save_to_image("C:\\Users\\Administrator\\Desktop\\cp_1h.png")
        pass
    
    def generate_13c_image(self):
        self.structure_menu_command("13C")
        time.sleep(1)
        self.save_to_image("C:\\Users\\Administrator\\Desktop\\cp_13c.png")
        pass
    
    def close_mol(self):
        close_alert = self.__getattribute__('close_alert')
        cat = threading.Thread(target=close_alert)
        cat.start()
        Image_handle = find_subHandle(self.Mhandle, [("MDIClient", 0), ("CSWDocument", 0)])
        win32gui.SendMessage(Image_handle, win32con.WM_CLOSE, 0, 0)
    
    
    def save_to_image(self, filePath):  
        Mhandle, confirmBTN_handle = self.file_menu_command('save_to_image')  
        EDIT_handle = find_subHandle(Mhandle, [("ComboBoxEx32", 0), ("ComboBox", 0), ("Edit", 0)])  # 定位保存地址句柄
        TYPE_handle = find_subHandle(Mhandle, [("ComboBox", 1)])
          
        print win32api.SendMessage(TYPE_handle, win32con.CB_SETCURSEL, 16, 0)
        time.sleep(0.5)
        if win32api.SendMessage(EDIT_handle, win32con.WM_SETTEXT, 0, os.path.abspath(filePath)) != 1:
            raise Exception("Set file opening path failed")
        time.sleep(0.5)
        win32api.SendMessage(Mhandle, win32con.WM_COMMAND, 1, confirmBTN_handle)
        time.sleep(0.5)
        close_alert = self.__getattribute__('close_alert')
        cat = threading.Thread(target=close_alert)
        cat.start()
        # 下面的方法会产生阻塞，所以需要开启新线程关闭窗口
        Image_handle = find_subHandle(self.Mhandle, [("MDIClient", 0), ("CSWDocument", 1)])
        win32gui.SendMessage(Image_handle, win32con.WM_CLOSE, 0, 0)
        return
    
    def close_alert(self):
        time.sleep(1)
        Alert_handle = win32gui.FindWindow("#32770", u"ChemBioDraw Ultra")
        if Alert_handle==0:
            #未弹出相应的窗口
            return
        print "Alert_handle:%x" % Alert_handle
        Btn_handle = find_subHandle(Alert_handle, [("Button", 1)])
        print "Btn_handle:%x" % Btn_handle
        # win32gui.PostMessage(Alert_handle, win32con.WM_COMMAND, Btn_handle, 0)
        win32gui.SetForegroundWindow(Alert_handle)  
        (left, top, right, bottom) = win32gui.GetWindowRect(Btn_handle)
        win32api.SetCursorPos((left + (right - left) / 2, top + (bottom - top) / 2)) 
        time.sleep(0.5)
        win32api.mouse_event(win32con.MOUSEEVENTF_LEFTDOWN, 0, 0) 
        time.sleep(0.05)
        win32api.mouse_event(win32con.MOUSEEVENTF_LEFTUP, 0, 0)
        time.sleep(0.05)
    


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="F:/log/nmr.log", help="NSQ topic")
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
    nmr.open_mol("C:\\Users\\Administrator\\Desktop\\molfile\\23672-07-3.mol")
    nmr.generate_1h_image()
    time.sleep(1)
    nmr.generate_13c_image()
    time.sleep(1)
    nmr.close_mol()
    time.sleep(1)
    # nmr.close_alert()
    # nmr.save_to_image("C:\\Users\\Administrator\\Desktop\\cp.png")
    logging.info(u'程序运行完成')
