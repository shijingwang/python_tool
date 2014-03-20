# -*- coding: utf-8 -*-
import types


class StringUtil(object):

    @staticmethod
    def s_length(_s_text):
        if _s_text == None:
            return 0
        if type(_s_text) is not types.UnicodeType:
            _s_text = unicode(_s_text)
        _s_text = _s_text.strip()

        s_length = 0
        for i in range(0, len(_s_text)):
            uchar = _s_text[i:i + 1]
            inside_code = ord(uchar)
            if inside_code < 0x0020 or inside_code > 0x7e:  # 不是半角字符就返回原来的字符
                s_length = s_length + 1
            else:
                s_length = s_length + 0.5
        return s_length

    # 解决截取时的奇数位的问题
    @staticmethod
    def sub(s, length):
        if type(s) is not types.UnicodeType:
            n_u = unicode(s)
        else:
            n_u = s
        return n_u[0:length]

    @staticmethod
    def is_english(_s_text):
        if _s_text == None:
            return False
        if type(_s_text) is not types.UnicodeType:
            _s_text = unicode(_s_text)
        _s_text = _s_text.strip()

        english = True
        for i in range(0, len(_s_text)):
            uchar = _s_text[i:i + 1]
            if (uchar >= u'\u0041' and uchar <= u'\u005a') or (uchar >= u'\u0061' and uchar <= u'\u007a'):
                english = True
            else:
                english = False
            if not english:
                break
        return english

    @staticmethod
    def get_english_number(_s_text):
        if _s_text == None:
            return False
        if type(_s_text) is not types.UnicodeType:
            _s_text = unicode(_s_text)
        _s_text = _s_text.strip()
        result = ''
        for i in range(0, len(_s_text)):
            uchar = _s_text[i:i + 1]
            if (uchar >= u'\u0041' and uchar <= u'\u005a') or (uchar >= u'\u0061' and uchar <= u'\u007a'):
                result = result + uchar
            if uchar in ['1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '.', '_', '-']:
                result = result + uchar
        return result

    @staticmethod
    def contain_chinese(_s_text):
        if _s_text == None:
            return False
        if type(_s_text) is not types.UnicodeType:
            _s_text = unicode(_s_text)
        _s_text = _s_text.strip()
        if len(_s_text) == 0:
            return False

        contain = False
        for i in range(0, len(_s_text)):
            if StringUtil.is_chinese(_s_text[i:i + 1]):
                contain = True
                break
        return contain

    @staticmethod
    def is_chinese(uchar):
        """判断一个unicode是否是汉字"""
        if uchar >= u'\u4e00' and uchar <= u'\u9fa5':
            return True
        else:
            return False

    @staticmethod
    def get_number_s(_s_text):
        if _s_text == None:
            return '0'
        if type(_s_text) is not types.UnicodeType:
            _s_text = unicode(_s_text)
        _s_text = _s_text.strip()
        result = ''
        for s in _s_text:
            if s in ['1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '.']:
                if s == '.' and '.' in result:
                    continue
                result = result + s
        if result == '':
            result = '0'
        return result

    @staticmethod
    def get_number_dot(_s_text):
        if _s_text == None:
            return '0'
        if type(_s_text) is not types.UnicodeType:
            _s_text = unicode(_s_text)
        _s_text = _s_text.strip()
        result = ''
        for s in _s_text:
            if s in ['1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '.']:
                result = result + s
        if result == '':
            result = '0'
        return result

    @staticmethod
    def get_datetime(_s_text):
        _s_text = _s_text.replace(u'年', '-').replace(u'月', '-').replace(u'日', ' ')
        _s_text = _s_text.replace(' - ', '-')
        return _s_text.strip()

if __name__ == '__main__':
    # r = StringUtil.sub(u'中国abcde', 1)
    # print r
    # r1 = StringUtil.sub(u'国困工dwd人遥发ewfsf', 5)
    # print r1
    # print StringUtil.contain_chinese(u'Gizli 中Şeyler')
    # print StringUtil.get_number('s2343.35.343d中国')
    # print StringUtil.get_number(u's2343.35.343d中国')
    print StringUtil.s_length('_s_text')
    print StringUtil.s_length('中国a')
