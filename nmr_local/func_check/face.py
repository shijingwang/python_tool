# -*- coding: utf-8 -*-
import logging
from tornado.options import define, options
import traceback
import win32gui
import win32con
import win32api
import os

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


class FaceGenWindow(object):
    def __init__(self, fgFilePath=None):
        self.Mhandle = win32gui.FindWindow("FaceGenMainWinClass", None)
        self.menu = win32gui.GetMenu(self.Mhandle)
        self.menu = win32gui.GetSubMenu(self.menu, 0)
        print "FaceGen initialization compeleted"

    # 然后定义一个菜单操作的方法：
    def menu_command(self, command):
        """
        菜单操作
        返回弹出的打开或保存的对话框的句柄 dig_handle
        返回确定按钮的句柄 confBTN_handle
        """
        command_dict = { 
            "open": [2, u"打开"],
            "save_to_image": [5, u"另存为"],
        }
        cmd_ID = win32gui.GetMenuItemID(self.menu, command_dict[command][0])
        win32gui.PostMessage(self.Mhandle, win32con.WM_COMMAND, cmd_ID, 0)
        for i in range(10):
            if win32gui.FindWindow(None, command_dict[command][1]): 
                break  # 如果找到了打开或者另存为的对话框，就跳出循环
            else:
                win32api.Sleep(200)  # 利用这个函数等待200ms，就不需要再额外导入time模块了
        dig_handle = win32gui.FindWindow(None, command_dict[command][1])
        confBTN_handle = win32gui.FindWindowEx(dig_handle, 0, "Button", None)
        print "dig_handle:%x button_handle:%x" % (dig_handle, confBTN_handle)
        return dig_handle, confBTN_handle

    def open_fg(self, fgFilePath):  
        """打开fg文件"""  
        Mhandle, confirmBTN_handle = self.menu_command('open')  
        handle = find_subHandle(Mhandle, [("ComboBoxEx32", 0), ("ComboBox", 0), ("Edit", 0)])  
        if win32api.SendMessage(handle, win32con.WM_SETTEXT, 0, os.path.abspath(fgFilePath).encode('gbk')) == 1:  
            return win32api.SendMessage(Mhandle, win32con.WM_COMMAND, 1, confirmBTN_handle)  
        raise Exception("File opening path set failed")  
    
    def save_to_image(self, filePath, format="jpg"):  
        format_dict = {  
            "bmp": 0,  # Facegen的Bug导致无法保存bmp  
            "jpg": 1,  
            "tga": 2,  
            "tif": 3,  
        }  
        Mhandle, confirmBTN_handle = self.menu_command('save_to_image')  
        mhandle = find_subHandle(Mhandle, [("DUIViewWndClassName", 0), ("DirectUIHWND", 0)])  
        EDIT_handle = find_subHandle(mhandle, [("FloatNotifySink", 0), ("ComboBox", 0), ("Edit", 0)])  # 定位保存地址句柄  
        PCB_handle = find_subHandle(mhandle, [("FloatNotifySink", 1)])  # 定位下拉菜单父窗体句柄  
        CB_handle = find_subHandle(PCB_handle, [("ComboBox", 0)])  # 定位下拉菜单窗体句柄  
        #wait_and_assert(EDIT_handle, find_subHandle(mhandle, [("FloatNotifySink", 0), ("ComboBox", 0), ("Edit", 0)]))  
        # 以下3行皆为ComboBox的list中选择格式必要的Message操作  
        if win32api.SendMessage(CB_handle, win32con.CB_SETCURSEL, format_dict[format], 0) == format_dict[format]:  
            win32api.SendMessage(PCB_handle, win32con.WM_COMMAND, 0x90000, CB_handle)  
            win32api.SendMessage(PCB_handle, win32con.WM_COMMAND, 0x10000, CB_handle)  
        else:  
            raise Exception("Change saving type failed")  
        # 填入保存地址，确认  
        if win32api.SendMessage(EDIT_handle, win32con.WM_SETTEXT, 0, os.path.abspath(filePath)) == 1:  
            return win32api.SendMessage(Mhandle, win32con.WM_COMMAND, 1, confirmBTN_handle)  
        raise Exception("Set file opening path failed")    
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
    nmr = FaceGenWindow()
    #nmr.menu_command("open")
    nmr.save_to_image("C:\\Users\\Administrator\\Desktop\\test")
    logging.info(u'程序运行完成')

