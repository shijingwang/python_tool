# -*- coding: utf-8 -*-
import logging
import xml.sax
from tornado.options import define, options
import sys
import os
import datetime
import time
import sc_setting

class ResultParse(xml.sax.ContentHandler):
    def __init__(self):
        self.current_data = ""
        self.test_result = {}
        self.current_result = {}
        self.parent_name = ''
        self.current_name = ''
        self.test_deep = 0;
        self.total_spend_time = 0
    
    # 元素开始事件处理
    def startElement(self, tag, attributes):
        self.current_data = tag
        # logging.info(tag)
        if tag == "test-suite":
            ttype = attributes.get('type', 'NoType')
            name = attributes.get('name', 'NoName')
            spend_time = attributes.get('time', 0)
            
            if ttype == 'Assembly':
                self.total_spend_time = spend_time
                pass
            elif ttype == 'Namespace':
                # 总的测试结果
                self.test_deep = 1
                self.parent_name=''
                self.current_name=''
            elif ttype == 'TestFixture':
                self.test_deep = 2
                self.test_result[name] = {}
                self.parent_name = name
                self.current_name=''
            else:
                self.test_deep = 3
                self.test_result[self.parent_name][name] = []
                self.current_name = name
        if tag == 'test-case' and self.current_name!='':
            name = attributes.get('name', '没有名称')
            result = attributes.get('result', 'Success')
            spend_time = attributes.get('time', 0)
            self.current_result['name'] = name
            self.current_result['result'] = result
            self.current_result['spend_time'] = spend_time
            self.current_result['msg'] = ''
            self.test_result[self.parent_name][self.current_name].append(self.current_result)
            
    
    # 元素结束事件处理
    def endElement(self, tag):
        self.current_result = {}
        self.current_data = {}
    
    # 内容事件处理
    def characters(self, content):
        # 错误数据暂时不做处理
        '''
        logging.info("tag:%s", self.current_data)
        if self.current_data == "stack-trace":
            self.current_result['msg'] = content
            logging.info("...name:%s   content:%s", self.current_result['name'], self.current_result['content'])
        '''
  
if (__name__ == "__main__"):
    reload(sys)
    sys.setdefaultencoding('utf-8')
    
    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')

    define("logfile", default="D:/parse.log", help="NSQ topic")
    options.parse_command_line()
    logfile = options.logfile
    logging.info(u'写入的日志文件为:%s', logfile)
    # 自动对日志文件进行分割
    fmt = '%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s'
    handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=100 * 1024 * 1024, backupCount=10)  # ʵ��handler
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt)  # 实例化formatter
    handler.setFormatter(formatter)  # 为handler添加formatter
    logging.getLogger('').addHandler(handler)
    
    # 创建一个 XMLReader
    parser = xml.sax.make_parser()
    # turn off namepsaces
    parser.setFeature(xml.sax.handler.feature_namespaces, 0)
    # 重写 ContextHandler
    result_parse = ResultParse()
    parser.setContentHandler(result_parse)
    target_path = u'D:/TestResult.%s.xml' % datetime.datetime.now().strftime('%Y.%m.%d %H.%M.%S')
    target_path = u'D:/TestResult%s.xml' % int(time.time()) 
    open(target_path, 'wb').write(open(sc_setting.test_result_path, 'rb').read())
    logging.info(u"测试结果:%s", os.path.exists(sc_setting.test_result_path))
    logging.info(u'TargetPath:%s', target_path)
    parser.parse(sc_setting.test_result_path)
    # logging.info(Handler.test_result)
    for key in result_parse.test_result:
        logging.info(u"测试节点名称:%s", key)
        keys = result_parse.test_result[key].keys() 
        keys.sort()
        for key1 in keys:
            logging.info(u'测试组名称:%s', key1)
            for value in result_parse.test_result[key][key1]:
                logging.info(u"name:%s result:%s msg:%s", value['name'], value['result'], value['msg'])
                
    
    
