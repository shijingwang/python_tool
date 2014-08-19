# -*- coding: utf-8 -*-
import re

class CasUtil(object):
    
    def __init__(self):
        self.cas_p = re.compile(r'([0-9]{2,7})[-—]{1}([0-9]{2})[-—]{1}([0-9]{1})')
    
    def cas_check(self, cas):
        match = self.cas_p.match(cas)
        if not match:
            return False
        cas1 = match.group(1)
        cas2 = match.group(2)
        cas3 = int(match.group(3))
        check_cas = cas1 + cas2
        check_cas = check_cas[::-1]
        counter = 0
        total = 0
        for c in check_cas:
            total = total + int(c) * (counter + 1)
            counter += 1
        if total % 10 == cas3:
            return True
        return False

if __name__ == '__main__':
    
    cu = CasUtil()
    print cu.cas_check('')
    print cu.cas_check('67214-05-5')
    print cu.cas_check('32780-06-6')
    print cu.cas_check('32780-06-7')