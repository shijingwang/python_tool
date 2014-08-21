# -*- coding: utf-8 -*-

import signal
import time

class TimeOut(object):
    
    def __init__(self):
        signal.signal(signal.SIGALRM, self.__getattribute__("handler"))
        pass
    
    def handler(self, signum, frame):
        print "Forever is over!"
        raise Exception("end of time")
    
    def loop_forever(self):
        while 1:
            print "sec"
            try:
                time.sleep(1)
            except:
                print 'Time out'
                break
    
    def process_data(self):
        for i in range(0, 100):
            signal.alarm(5)
            print '第%s次运行' % i
            self.loop_forever()

if __name__ == '__main__':
    to = TimeOut()
    to.process_data()
    print 'Finish'
