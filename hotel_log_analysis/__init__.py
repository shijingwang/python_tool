# -*- coding: utf-8 -*-

import sys

class LogAnalysis(object):
    
    def __init__(self):
        pass
    
    def read_file(self):
        f = open("D:/nohup.out")  # 返回一个文件对象
        line = f.readline()  # 调用文件的 readline()方法
        time_list = []
        while line:
            # print line,                 # 后面跟 ',' 将忽略换行符
            # print(line, end = '')　　　# 在 Python 3中使用
            
            if 'get URL--cost:' in line and 'qunar' in line:
                a = line.find("get URL--cost:")
                b = line.find("ms http")
                spend = int(line[a+14:b].strip())
                time_list.append(spend)
            if 'get URL2--cost:' in line and 'qunar' in line:
                a = line.find("get URL2--cost:")
                b = line.find("ms http")
                spend = int(line[a+15:b].strip())
                time_list.append(spend)
            if 'get URL3--cost:' in line and 'qunar' in line:
                a = line.find("get URL3--cost:")
                b = line.find("ms http")
                spend = int(line[a+15:b].strip())
                time_list.append(spend)
            line = f.readline()
        f.close()
        time_list.sort(reverse=True)
        total = 0
        for t in time_list:
            total = total + t
        print total/len(time_list)
        print len(time_list)

if __name__ == '__main__':

    reload(sys)
    sys.setdefaultencoding('utf-8')
    la = LogAnalysis()
    la.read_file()
