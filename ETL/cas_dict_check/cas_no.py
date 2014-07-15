# -*- coding: utf-8 -*-
import re
regex = r"[0-9]{2,7}[-]{1}[0-9]{2}[-]{1}[0-9]{1}$"
p = re.compile(regex)
print p.match('1234-12-1')
print p.match('1234-1-1')
print p.match('56-40-6 (Parent)')
print p.match('56-40-6abc')
