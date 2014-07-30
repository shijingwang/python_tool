# -*- coding: utf-8 -*-
from tornado.options import define, options
import logging
from bs4 import BeautifulSoup

class HtmlFormat(object):
    
    def cn_format_to_file(self):
        fp = "/home/kulen/Documents/设计样式/96.html"
        fpw = "/home/kulen/Documents/设计样式/96_w2.html"
        fp = "/home/kulen/Documents/设计样式/1.html"
        fpw = "/home/kulen/Documents/设计样式/1_w2.html"
        fp_writer = open(fpw, 'w')
        start = '''<html>
<head>
<meta charset="utf-8"/>
</head>
<body>'''
        fp_writer.write(start)
        fp_writer.write(self.cn_format(fp))
        stop = '''</body>
</html>'''
        fp_writer.write(stop)
        fp_writer.close()
        
    def cn_format(self, source):
        fp = source
        fp_reader = open(fp)
        html_content = ''
        while 1:
            line = fp_reader.readline()
            if not line:
                break
            html_content += line
        fp_reader.close()
        # print html_content
        check_title_list = [u'2.对环境的影响:', u'3.现场应急监测方法:', u'4.实验室监测方法:', u'5.环境标准:', u'6.应急处理处置方法:']
        soup = BeautifulSoup(html_content);
        '''
        divs = soup.find_all("div")
        for div in divs:
            div.unwrap()
        ps = soup.find_all("p")
        for p in ps:
            p.unwrap()
        '''
        fonts = soup.find_all("font")
        for font in fonts:
            font.unwrap()
        soup = BeautifulSoup(str(soup))
        trs = soup.find_all("tr")
        table = soup.find('table')
        table['class'] = '_table_all'
        cl = 0
        for tr in trs:
            td = tr.find('td', {'colspan':'2'})
            if not td:
                cl += 1
                tr['class'] = '_tr_' + str(cl)
                td1 = tr.find_all('td')[0]
                td2 = tr.find_all('td')[1]
                td1['class'] = "_td_header_" + str(cl)
                td2['class'] = "_td_header_content_" + str(cl)
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
                    # print check_value
                    # print '------------------'
                    if check_value in check_title_list:
                        counter += 1
                        child = child.wrap(soup.new_tag('b'))
                        child ['class'] = "_b_title_" + str(counter)
                        if counter > 1 :
                            hr_tag = soup.new_tag("hr")
                            hr_tag['class'] = '_hr_split_' + str(counter)
                            child.insert_before(hr_tag)
                    previous = check_value
                # print type(td)
                del td['colspan']
                td.name = 'div'
                td['class'] = '_div_content'
                # print td.string.wrap(soup.new_tag('div'))
                table.insert_after(td)
        self.delete_all_link(soup) 
        v = str(soup)
        soup = BeautifulSoup(v);
        divs = soup.find_all("div")
        for div in divs:
            check_value = unicode(div.get_text().replace('\n', '').strip())
            if check_value in check_title_list:
                div.name = 'b'
                child = div
                child ['class'] = "_b_title_" + str(counter)
                hr_tag = soup.new_tag("hr")
                hr_tag['class'] = '_hr_split_' + str(counter)
                child.insert_before(hr_tag)
        ps = soup.find_all("p")
        for p in ps:
            check_value = unicode(p.get_text().replace('\n', '').strip())
            p.string = check_value
            if check_value in check_title_list:
                p.name = 'b'
                child = p
                child ['class'] = "_b_title_" + str(counter)
                hr_tag = soup.new_tag("hr")
                hr_tag['class'] = '_hr_split_' + str(counter)
                child.insert_before(hr_tag)
        v = str(soup)
        soup = BeautifulSoup(v);
        divs = soup.find_all("div")
        for div in divs:
            if 'class' in div:
                continue
            br_tag = soup.new_tag("br")
            div.insert_before(br_tag)
            div.unwrap()
        v = str(soup)
        rv = '''
<br/>
      
    '''
        v = v.replace(rv, '<br/>')
        rv = '''<br/>
  '''
        v = v.replace(rv, '<br/>')
        v = v.replace('<br/>  ', '<br/>')
        rv = '''<br/>
<br/>'''
        v = v.replace(rv, '<br/>')
        soup = BeautifulSoup(v);
        bs = soup.find_all("b")
        counter = 0
        for b in bs:
            counter += 1
            b['class'] = "_b_title_" + str(counter)
        hrs = soup.find_all("hr")
        counter = 0
        for hr in hrs:
            counter += 1
            hr['class'] = "_hr_split_" + str(counter)
        v = str(soup)
        print 'Finish'
        return v

    def en_format_to_file(self):
        fp = "/home/kulen/Documents/设计样式/2.html"
        fpw = "/home/kulen/Documents/设计样式/2_w2.html"
        fp_writer = open(fpw, 'w')
        fp_writer.write(self.en_format(fp))
        fp_writer.close()
    
    def en_format(self, source):
        fp = source
        fp_reader = open(fp)
        html_content = ''
        while 1:
            line = fp_reader.readline()
            if not line:
                break
            html_content += line
        fp_reader.close()
        # print html_content
        soup = BeautifulSoup(html_content);
        table = soup.find('table')
        table['class'] = '_en_table_1'
        strongs = table.find_all('strong')
        for strong in strongs:
            strong.unwrap()
        
        tables = soup.find_all('table')
        counter = 0
        table_sec2 = None
        for _table in tables:
            counter += 1
            if counter == 1:
                trs = _table.find_all('tr')
                trl = 0
                for tr in trs:
                    trl += 1
                    tr['class'] = "_en_1_tr_" + str(trl)
                    td1 = tr.find_all('td')[0]
                    td2 = tr.find_all('td')[1]
                    td1['class'] = "_en_1_td_header" + str(trl)
                    td2['class'] = "_en_1_td_header_content" + str(trl)
                continue
            if counter == 4:
                table_sec2 = _table
                table_sec2['class'] = '_en_table_2'
                trs = _table.find_all('tr')
                trl = 0
                tdl = 0
                for tr in trs:
                    trl += 1
                    tr['class'] = "_en_2_tr_" + str(trl)
                    tds = tr.find_all('td')
                    for td in tds:
                        tdl += 1
                        td['class'] = "_en_2_td_" + str(tdl)
                continue
            trs = _table.find_all('tr')
            for tr in trs:
                tbodys = _table.find_all('tbody')
                for tbody in tbodys:
                    tbody.unwrap()
                tds = tr.find_all('td')
                for td in tds:
                    td.unwrap()
                tr.unwrap()
            _table.unwrap()
            
        strongs = soup.find_all('strong')
        counter = 0
        for strong in strongs:
            counter += 1
            if counter > 1:
                strong['class'] = '_en_s_title'
                hr_tag = soup.new_tag("hr")
                hr_tag['class'] = '_en_hr_split'
                strong.insert_before(hr_tag)
                br_tag = soup.new_tag("br")
                strong.insert_after(br_tag)
        self.delete_all_link(soup)
        v = str(soup)
        v = v.replace("<br/> <br/>", "<br/>")
        rv = '''<br/>


 
<br/>'''
        v = v.replace(rv, "<br/>")
        rv = '''<br/>


 '''
        v = v.replace(rv, "<br/>")
        return v
    
    def delete_all_link(self, soup):
        links = soup.find_all('a')
        for link in links:
            link.unwrap()

if __name__ == '__main__':

    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="/home/kulen/log/run.log", help="NSQ topic")
    define("func_name", default="spider_apple")
    options.parse_command_line()
    logfile = options.logfile
    func_name = options.func_name
    logging.info(u'写入的日志文件为:%s', logfile)
    
    cs = HtmlFormat()
    cs.en_format_to_file()
    #cs.cn_format_to_file()
    
