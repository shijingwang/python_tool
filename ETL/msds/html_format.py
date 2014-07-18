# -*- coding: utf-8 -*-
from tornado.options import define, options
import logging
from bs4 import BeautifulSoup

class HtmlFormat(object):
    
    def cn_format(self):
        fp = "/home/kulen/Documents/设计样式/1.html"
        fpw = "/home/kulen/Documents/设计样式/1_w2.html"
        fp_reader = open(fp)
        html_content = ''
        while 1:
            line = fp_reader.readline()
            if not line:
                break
            html_content += line
        fp_reader.close()
        # print html_content
        fp_writer = open(fpw, 'w')
        soup = BeautifulSoup(html_content);
        trs = soup.find_all("tr")
        table = soup.find('table')
        table['class'] = '_table_all'
        for tr in trs:
            td = tr.find('td', {'colspan':'2'})
            if not td:
                td1 = tr.find_all('td')[0]
                td2 = tr.find_all('td')[1]
                td1['class'] = "_td_header"
                td2['class'] = "_td_header_content"
            else:
                tr.extract()
                counter = 0
                previous = ''
                for child in td.children:
                    check_value = unicode(child).strip()
                    if len(check_value) == 0:
                        continue
                    if check_value == u'<br/>' and previous == u'<br/>':
                        # child.extract()
                        # previous = ''
                        pass
                    print check_value
                    print '------------------'
                    if check_value in [u'2.对环境的影响:', u'3.现场应急监测方法:', u'4.实验室监测方法:', u'5.环境标准:', u'6.应急处理处置方法:']:
                        counter += 1
                        child = child.wrap(soup.new_tag('b'))
                        child ['class'] = "_b_title"
                        if counter > 1 :
                            hr_tag = soup.new_tag("hr")
                            hr_tag['class'] = '_hr_split'
                            child.insert_before(hr_tag)
                    previous = check_value
                print type(td)
                del td['colspan']
                td.name = 'div'
                # print td.string.wrap(soup.new_tag('div'))
                table.insert_after(td)
                
        v = str(soup)
        fp_writer.write(v)
        fp_writer.close()
        print 'Finish'
    
    def en_format(self):
        fp = "/home/kulen/Documents/设计样式/2.html"
        fpw = "/home/kulen/Documents/设计样式/2_w2.html"
        fp_reader = open(fp)
        html_content = ''
        while 1:
            line = fp_reader.readline()
            if not line:
                break
            html_content += line
        fp_reader.close()
        # print html_content
        fp_writer = open(fpw, 'w')
        soup = BeautifulSoup(html_content);
        table = soup.find('table')
        table['class'] = '_en_table_all'
        trs = table.find_all('tr')
        for tr in trs:
            td1 = tr.find_all('td')[0]
            td2 = tr.find_all('td')[1]
            td1['class'] = "_en_td_header"
            td2['class'] = "_en_td_header_content"
        strongs = table.find_all('strong')
        for strong in strongs:
            strong.name = ''
        
        tables = soup.find_all('table')
        counter = 0
        table_sec2 = None
        for _table in tables:
            counter += 1
            if counter == 4:
                table_sec2 = _table
            _table['width'] = '100%' 
        strongs = soup.find_all('strong')
        counter = 0
        for strong in strongs:
            counter += 1
            if counter > 1:
                strong['class'] = '_en_s_title'
                hr_tag = soup.new_tag("hr")
                hr_tag['class'] = '_en_hr_split'
                strong.insert_before(hr_tag) 
        table_sec2['class'] = '_en_table_sec2'   
        v = str(soup)
        fp_writer.write(v)
        fp_writer.close()
        print 'Finish'

if __name__ == '__main__':

    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="/home/kulen/log/run.log", help="NSQ topic")
    define("func_name", default="spider_apple")
    options.parse_command_line()
    logfile = options.logfile
    func_name = options.func_name
    logging.info(u'写入的日志文件为:%s', logfile)
    
    cs = HtmlFormat()
    cs.en_format()
    cs.cn_format()
