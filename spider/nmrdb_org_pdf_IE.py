# coding: utf-8
# filename: nmrdb_org_pdf_IE.py
# Memo: 控制(中文)IE浏览器来访问http://www.nmrdb.org/并下载pdf文档
# date: 2014-06-26

import sys
import os
os.chdir(os.path.dirname(os.path.realpath(sys.argv[0])))
import re
import csv
import time
import urllib
import logging
import datetime
from webscraping import settings
settings.log_file = settings.log_file.replace('webscraping.log', 'webscraping_%s.log' % datetime.datetime.now().strftime('%Y-%m-%d'))
from webscraping import adt, common, download, xpath
from collections import deque
import win32gui, win32con, win32api, winGuiAuto 
import cPAMIE
import random
import itertools
from tornado.options import define, options
import hashlib
from bs4 import BeautifulSoup
from down_util2 import DownUtil2

DELAY = 5
TIME_OUT = 30
MAX_TIME_OUT = 45

DOMAIN = 'http://www.nmrdb.org/'

Fields = ['smiles', 'pdf', 'ACS assignment']

DEBUG = '--debug' in sys.argv

HIDE_IE_WINDOW = '--hideie' in sys.argv

for d in ['csvs', 'pdfs', 'htmls']:
    if not os.path.exists(d): 
        os.mkdir(d)

def elapsed(start):
    seconds = time.time() - start
    return seconds

def click_popup_window_OK_button(title=''):
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

    return found_OK_button
    
Illegal_signs_list = [' ', '\\', '/', ':', '*', '?', '"', '<', '>', '|', '\n']
def replace_illegal_signs(s, replace_with='_', signs_list=Illegal_signs_list):
    '''替换掉字符串（比如用作文件名的字符串）中的非法字符
       usage example: resut_string = replace_illegal_signs(s=s, replace_with='_', signs_list=Illegal_signs_list)
    '''
    for sign in signs_list:
        s = s.replace(sign, replace_with)
    return s

def download_and_save_pdf(D, url, pdf_path):
    if not os.path.exists(pdf_path):
        try:
            download_counter = 2
            while True:
                bytes = D.get(pdf_url, read_cache=False)  # download pdf
                if bytes:
                    break
                download_counter -= 1
                if download_counter == 0:
                    break
            if bytes:
                open(pdf_path, 'wb').write(bytes)  # save pdf
            else:
                common.logger.info("Download pdf failed. pdf_url: %s" % pdf_url)  # pdf download failed.
        except Exception, e:
            print e
            print "Error occurred in download and save pdf!"  # pdf download failed. 

def backup_and_rewrite_csv(source_file='', items_dict=adt.HashDict(int)):
    '''重写指定的csv文件，目的是去除文件末尾的不完全行，以免在此后读取该csv文件的数据行时发生csv.Error: newline inside string的错误。
    '''
    try:
        input_csv = source_file
        if not os.path.exists(input_csv):
            print 'File %s does not exist.' % input_csv
            return True
        
        print 'Backup and rewriteing %s ......' % input_csv
        output_csv = re.sub(r'\.csv$', '_tmp.csv', input_csv)
        writer = common.UnicodeWriter(output_csv, mode='wb')
        f_csv = open(input_csv, 'rb')
        num = 0
        try:
            for row in csv.reader(f_csv):
                num += 1
                if not row[0] in items_dict:
                    items_dict[row[0]]
                writer.writerow(row)
        except csv.Error, e:
            print 'csv.Error: %s' % str(e)
        finally:
            f_csv.close()
        writer.flush()
        writer.close()
        print 'Rewrite %s finished. total %d row writed.' % (input_csv, num)
        bak_csv = re.sub(r'\.csv$', '_bak.csv', input_csv)
        if os.path.exists(bak_csv):
            os.remove(bak_csv)  # 删除之前的同名备份文件
        os.rename(input_csv, bak_csv)  # 将当前的输入文件改名为备份文件
        os.rename(output_csv, input_csv)  # 将当前的输出文件改名为输入文件
    except Exception, e:
        common.logger.error('Error occured inside backup_and_rewrite_csv(). %s' % str(e))
        return False
    else:
        return True

def scrape_nmrdb_org_pdf():
    fn_smiles = 'smiles_list.csv'
    if not os.path.exists(fn_smiles):
        common.logger.info('File %s does not exist.' % fn_smiles)
        return
    
    common.logger.info('Loading smiles parameters from %s ......' % fn_smiles)
    smiles_items = deque()
    row_num = 0
    f_smiles = open(fn_smiles, 'rb')
    for row in csv.reader(f_smiles):
        row_num += 1
        smiles_item = row[0].strip()
        if row_num == 1 or row_num % 5000 == 0:
            print '[smiles %d] %s' % (row_num, smiles_item)
        if smiles_item and not smiles_item in smiles_items:
            smiles_items.append(smiles_item)
    print '[smiles %d] %s' % (row_num, smiles_item)
    f_smiles.close()
    common.logger.info('Total %d smiles parameters loaded.' % len(smiles_items))
    
    result_csv = 'csvs/nmrdb_org_pdf.csv'
    processed = adt.HashDict(int)   
    backup_and_rewrite_csv(source_file=result_csv, items_dict=processed)  # backup and repair result csv file
    
    use_cache = DEBUG
    D = download.Download(proxy_file='proxies.txt', num_retries=1, delay=DELAY, timeout=45, read_cache=use_cache, write_cache=use_cache)
    writer = common.UnicodeWriter(result_csv, encoding='utf-8', mode='ab', unique=True)
    if not writer.rows:
        writer.writerow(Fields)
    writer2 = common.UnicodeWriter('csvs/nmrdb_org_pdf_failed.csv', encoding='utf-8', mode='wb', unique=True)
    num_counter = itertools.count(1).next
    do_num = 0
    
    def startup_ie():    
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
                    click_popup_window_OK_button(title=u"iexplore.exe - 应用程序错误".encode('gbk'))
                    time.sleep(2)
                    continue
                return ie
            except Exception, e:
                print e
            if elapsed(start) > 30:
                break
            try_num += 1
            if try_num > 5:
                break
            time.sleep(2)
            
        return None

    def quit_ie(ie):
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
            if elapsed(start) > 60:
                common.logger.error('Quit IE failed.')
                break
        ie = None
        return ie
    
    def close_alter_window(elapsed_seconds=1):
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
            if elapsed(start) > 10:
                break
            time.sleep(1)   

    common.logger.info('Start scraping ......')
    # 先启动IE
    ie = startup_ie()
    if not ie:
        common.logger.error('Failed to start up IE.')
        return
    
    # 开始PDF下载流程
    startup = time.time()
    while True:
        curr_start = time.time()
        try:
            smiles_item = smiles_items.popleft()
            smiles_para = urllib.quote_plus(smiles_item)
            count_num = num_counter()
            print '[%d] processing smiles: %s' % (count_num, smiles_item)
            # common.logger.info('[%d] processing smiles: %s' % (count_num, smiles_item))
        except IndexError:
            break
        
        pdf_fname = replace_illegal_signs(smiles_item, replace_with='_', signs_list=Illegal_signs_list)
        if len(pdf_fname) > 170:
            pdf_fname = '%s_%s' % (pdf_fname[:170], hashlib.md5(smiles_item).hexdigest())
        else:
            pdf_fname = '%s_%s' % (pdf_fname, hashlib.md5(smiles_item).hexdigest())
        pdf_path = 'pdfs/%s.pdf' % pdf_fname 
        if os.path.exists(pdf_path) and smiles_item in processed:
            continue
        
        do_num += 1
        url = 'http://www.nmrdb.org/predictor?smiles=%s' % smiles_para
        url = 'C:\\Users\\Administrator\\Desktop\\nmr_origin\\predictor.htm'
        # url = 'http://localhost:8080/nmr/predictor.htm'                                                                                                             
        hwd_ie = winGuiAuto.findTopWindows("Windows Internet Explorer")
        if not ie or not hwd_ie:
            ie = startup_ie()
            if not ie: 
                writer2.writerow([smiles_item])
                writer2.flush()
                common.logger.error('Start up IE failed. smiles_item[%d]: %s' % (count_num, smiles_item))
                continue
            hwd_ie = winGuiAuto.findTopWindows("Windows Internet Explorer")
        
        # step1: 打开请求入口页面，等待页面开始载入数据(java程序运算中)，大概等待15秒左右，数据载入成功。
        ie.navigate(url)
        # time.sleep(1)

        # 首次打开请求入口页面，会弹出一个安全警告窗口，要点击“运行”按钮关闭窗口。
        # time.sleep(1)
        # close_alter_window()
        
        
        
        # step2: 点击Get PDF，下载PDF文件。
        start = time.time()
        while hwd_ie:
            try:
                # time.sleep(2)
                hwnd = winGuiAuto.findTopWindows(u"安全警告".encode('gbk'))
                if hwnd:
                    close_alter_window(elapsed_seconds=0)
                # 点击链接获取分子结构和图形
                get_data_link = ie.elementFind('a', 'name', 'getresultdata')
                ie.elementClick(get_data_link)
                time.sleep(5)
                text_tableformat_value = ie.textAreaGetValue('tableformat', 'value')
                if text_tableformat_value:
                    btn_get_pdf = ie.elementFind('input', 'value', 'Get PDF')
                    ie.elementClick(btn_get_pdf)
                    if HIDE_IE_WINDOW:
                        ie._ie.Visible = 0
                    break
                text_ethylvinylether_value = ie.textAreaGetValue('ethylvinylether', 'value')
                if text_ethylvinylether_value:
                    btn_get_pdf = ie.elementFind('input', 'value', 'Get PDF')
                    ie.elementClick(btn_get_pdf)
                    if HIDE_IE_WINDOW:
                        ie._ie.Visible = 0
                    break
            except Exception, e:
                print e
            if elapsed(start) > 30:
                # common.logger.error('[Step 2] Waiting too long. smiles_item[%d]: %s' % (count_num, smiles_item))
                break
            hwd_ie = winGuiAuto.findTopWindows("Windows Internet Explorer")
        time.sleep(5)
        
        # step3: 点击下载按钮，下载并保存PDF到本地。
        download_try_num = 0
        start = time.time()
        hwd_ie = winGuiAuto.findTopWindows("Windows Internet Explorer")
        while hwd_ie:
            is_invalid_pdf = False
            hwnd = winGuiAuto.findTopWindows(u"安全警告".encode('gbk'))
            
            if hwnd:
                close_alter_window(elapsed_seconds=0)
            try:
                html = ie.outerHTML()
            except Exception, e:
                html = ''
                common.logger.error('[Step 3] Failed to get html. smiles_item[%d]: %s \nError message: %s' % (count_num, smiles_item, str(e)))
            time.sleep(1)
            if 'action=/cheminfo/servlet/org.cheminfo.hook.appli.HookServlet' in html or 'action=http://www.nmrdb.com/cheminfo/servlet/org.cheminfo.hook.appli.HookServlet':
            # if  1==1:
                # print html
                soup = BeautifulSoup(html)
                para_molfile = xpath.get(html, '//TEXTAREA[@name=molfile]').strip()
                #para_molfile = soup.find('TEXTAREA', {'name':'molfile'})
                print '-------------------------molfile'
                print para_molfile
                para_ethylvinylether = xpath.get(html, '//TEXTAREA[@name=ethylvinylether]').strip()
                para_assignment = xpath.get(html, '//TEXTAREA[@name=assignment]').strip()
                #para_assignment = soup.find('TEXTAREA', {'name':'assignment'})
                print '-------------------------assignment'
                print para_assignment
                para_tableformat = xpath.get(html, '//TEXTAREA[@name=tableformat]').strip()
                #para_tableformat = soup.find('TEXTAREA', {'name':'tableformat'})
                print '-------------------------tableformat'
                print para_tableformat
                para_url = xpath.get(html, '//input[@name=url]/@value').replace('&amp;', '&').strip()
                para_url = soup.find('input', {'name':'url'})['value']
                print '-------------------------url'
                print para_url
                para_xmlString = xpath.get(html, '//INPUT[@name=xmlString]/@value').strip()
                para_xmlString = soup.find("input", {'name':'xmlString'})['value']
                print '-------------------------xmlString'
                print para_xmlString
                
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
                du = DownUtil2()
                du.downfile(get_pdf_url, post_data2, 'F:/file/', '.pdf')
                '''
                pdf_html = D.get(get_pdf_url, data=post_data2)
                
                if pdf_html and pdf_html.startswith('%PDF'):
                    open(pdf_path, 'wb').write(pdf_html)
                else:
                    if D.error_content and 'HTTP Status 500' in D.error_content: 
                        is_invalid_pdf = True
                if os.path.exists(pdf_path):
                    row = [smiles_item, pdf_path, para_assignment]
                    writer.writerow(row)
                    writer.flush()
                    if not smiles_item in processed:
                        processed[smiles_item]
                    break
                if is_invalid_pdf:
                    break   
                download_try_num += 1
                if download_try_num >= 2:
                    break
                '''
            if elapsed(start) > 45:
                common.logger.error('[Step 3] Waiting too long. smiles_item[%d]: %s' % (count_num, smiles_item))
                break
            time.sleep(1)
            hwd_ie = winGuiAuto.findTopWindows("Windows Internet Explorer")
          
        if not os.path.exists(pdf_path):    
            writer2.writerow([smiles_item])  
            writer2.flush()
            common.logger.error('PDF download failed. smiles_item[%d]: %s' % (count_num, smiles_item))
            
        # step4: 关闭打开的IE浏览器，为下次处理作准备。
        if is_invalid_pdf or do_num % 10 == 0:
            ie = quit_ie(ie)
        time.sleep(1)
            
    writer.close()
    writer2.close()
    if ie:
        quit_ie(ie) 

    common.logger.info('\nScraping %s over!' % DOMAIN)
    total_seconds = elapsed(startup)
    common.logger.info('Total time: %d hours %d minutes %d seconds, do smiles_items: %d. Everage speed: %6.2f seconds per smiles_item.' \
                       % (total_seconds / 3600, (total_seconds % 3600) / 60, (total_seconds % 3600) % 60, do_num, total_seconds / (do_num * 1.0)))

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
    scrape_nmrdb_org_pdf()
    
