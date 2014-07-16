# -*- coding: utf-8 -*-
import winGuiAuto
import win32gui
import win32api
import win32con
import time

w1hd = win32gui.FindWindow(0, u"ChemBioDraw Ultra")
print w1hd
print type(w1hd)
w2hd = win32gui.FindWindowEx(w1hd, None, None, None)
print w2hd
print type(w2hd)

# 获取窗口焦点
#win32gui.SetForegroundWindow(w2hd)
# 快捷键Alt+F
#win32api.keybd_event(18, 0, 0, 0)  # Alt
#win32api.keybd_event(70, 0, 0, 0)  # F

#win32api.keybd_event(70,0,win32con.KEYEVENTF_KEYUP,0)  #释放按键
#win32api.keybd_event(18,0,win32con.KEYEVENTF_KEYUP,0)

wdname3=u"open"
