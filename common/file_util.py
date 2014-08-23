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
