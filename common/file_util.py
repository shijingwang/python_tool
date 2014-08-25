# -*- coding: utf-8 -*-

import os

class FileUtil(object):
    
    def __init__(self):
        pass

    def delete_file(self, fp):
        try:
            if os.path.exists(fp):
                os.remove(fp)
        except Exception:
            pass

    def copy_file(self, source_file, target_file):
        try:
            target_dir = target_file[0:target_file.rfind('/')]
            if not os.path.exists(target_dir):
                os.makedirs(target_dir)
            open(target_file, 'wb').write(open(source_file, 'rb').read())
        except Exception:
            pass

if __name__ == '__main__':
    
    fu = FileUtil()
    fu.copy_file('/home/kulen/molpic2/00/00/26.png', '/home/kulen/molpic/00/00/2.6.png')