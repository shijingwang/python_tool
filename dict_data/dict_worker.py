# -*- coding: utf-8 -*-
from tornado.options import define, options
import logging
import os, sys
import csv
import signal
import traceback
import time
try:
    import python_tool
except ImportError:
    fp = os.path.abspath(__file__)
    sys.path.append(fp[0:fp.rfind('python_tool') + 11])
from common.con_util import ConUtil
from common.cas_util import CasUtil
import dict_conf

class DictWorker(object):
    
    def __init__(self):
        self.db_dict = ConUtil.connect_mysql(dict_conf.MYSQL_DICT_WORKER)
        self.db_dict_source = ConUtil.connect_mysql(dict_conf.MYSQL_DICT_SOURCE)
        if not os.path.exists(dict_conf.worker_bitmapdir):
            os.makedirs(dict_conf.worker_bitmapdir)
        self.tmp_mol1 = '/tmp/mol1.mol';self.tmp_mol2 = '/tmp/mol2.mol'
        self.i_mol_id = self.get_start_molid()
        self.cu = CasUtil()
        signal.signal(signal.SIGALRM, self.__getattribute__("handler"))
        sql = 'select fpdef from moldb_fpdef'
        rs = self.db_dict.query(sql)
        for r in rs:
            self.fpdef = r['fpdef']
        # print self.fpdef
        pass

    def delete_file(self, fp):
        try:
            if os.path.exists(fp):
                os.remove(fp)
        except Exception:
            pass
    
    def handler(self, signum, frame):
        raise Exception(u"Process Timeout")
        
    def import_sdf(self, sdf_file):
        fp = sdf_file
        logging.info("导入sdf文档数据:%s", fp)
        fp_reader = open(fp)
        mol = ''
        name = ''
        value = ''
        attr_list = []
        export_list = []
        goods_list = []
        goods_dict = {}
        prices = []
        counter = 0
        while 1:
            line = fp_reader.readline()
            if not line:
                counter += 1
                if counter >= 20:
                    break
            # print '=======' + line
            if line.startswith('>  <') or line.startswith('$$$$'):
                if name:
                    value = value.replace('\n', '').replace('\r', '')
                    # print "Name:%s Value:%s" % (name, value)
                    attr_list.append({'name':name, 'value':value})
                    if name in ['spec_1', 'spec_2', 'spec_3', 'spec_4', 'spec_5']:
                        prices.append(value)
                    # 商品价格表数据
                    goods_dict[name] = value
                if line.startswith('>  <'):
                    name = ''
                    value = ''
                    counter = 0
                    name = line[line.index('<') + 1:line.rindex('>')]
                    continue
            if name:
                value += line
            else:
                mol += line
            check_line = line.replace('\n', '').replace('\r', '')
            
            # print '---------------------'
            # print '[%s]' % line
            if check_line == '$$$$':
                # print attr_list
                # print mol
                # 已经完成对一个化合物数据的提取
                try:
                    v_d = {}
                    for key in dict_conf.SDF_KEY:
                        for attr in attr_list:
                            if attr['name'] in dict_conf.SDF_KEY[key]:
                                v_d[key] = attr['value']
                    query_mol_id = -1
                    if v_d['cas_no'] in self.fix_cas:
                        query_mol_id = self.write_dic(v_d, mol)
                    sql = 'select * from search_moldata where mol_id=%s'
                    sql = sql % query_mol_id
                    
                    rs = self.db_dict.query(sql)
                    for r in rs:
                        export_list.append((r['mol_id'], r['cas_no']))
                    for price in prices:
                        goods_list.append((r['mol_id'], r['cas_no'], goods_dict.get('PURITY', ''), goods_dict.get('LEAD_TIME', ''), goods_dict.get('STOCK', ''), goods_dict.get('CAPACITY', ''), price))
                except Exception, e:
                    logging.error(u"处理产品时出错:%s", attr_list)
                    logging.error(traceback.format_exc())
                attr_list = []
                mol = ''
                name = ''
                value = ''
                counter = 0
                goods_dict = {}
                prices = []
                # break
        fp_reader.close()
        
        file_name = fp[fp.rfind('/') + 1:fp.rfind('.')]
        self.write_csv(file_name, ['mol_id', 'cas_no'], export_list)
        self.write_csv(file_name + '_goods', ['mol_id', 'cas_no', 'PURITY', 'LEAD_TIME', 'STOCK', 'CAPACITY', 'price'], goods_list)
    
    def write_csv(self, file_name, columns, data_list):
        result_file_fp = dict_conf.SDF_RESULT_PATH + file_name + ".csv"
        self.delete_file(result_file_fp)
        csvfile = file(result_file_fp, 'wb')
        writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow(columns)
        writer.writerows(data_list)
        csvfile.close()
    
    def get_start_molid(self):
        sql = 'select max(mol_id) as mol_id from search_moldata'
        rs = self.db_dict.query(sql)
        for r in rs:
            mol_id = r['mol_id']
        if not mol_id:
            mol_id = 0
        return mol_id
    
    def write_dic(self, data_dict, mol):
        logging.info(u"处理属性:%s数据", data_dict)
        if not self.cu.cas_check(data_dict['cas_no']):
            return -1
        self.delete_file(self.tmp_mol1)
        mol1_writer = open(self.tmp_mol1, 'w')
        mol1_writer.write(mol)
        mol1_writer.close()
        check_mol_id = self.check_match(data_dict['cas_no'], mol)
        # 字典中有相应的数据
        if check_mol_id > 0:
            mol_id = check_mol_id
            return check_mol_id
        else:
            self.i_mol_id = self.i_mol_id + 1
            mol_id = self.i_mol_id
        if not data_dict.get('name_en'):
            data_dict['name_en'] = ''
        if not data_dict.get('name_en_alias'):
            data_dict['name_en_alias'] = ''
        if not data_dict.get('name_cn'):
            data_dict['name_cn'] = ''
        if not data_dict.get('name_cn_alias'):
            data_dict['name_cn_alias'] = ''
        params = [mol_id]
        params.append(data_dict['name_en'])
        params.append(data_dict['name_en_alias'])
        params.append(data_dict['name_cn'])
        params.append(data_dict['name_cn_alias'])
        params.append(data_dict['cas_no'])
        c = "obprop %s 2>/dev/null | awk -F\"\\t\" '{print $1}' | cut -c 17- | head -16 | tail -15"
        c = c % (self.tmp_mol1)
        result = os.popen(c).read()
        results = result.split('\n');
        for i in range(0, 15):
            v = results[i].strip()
            if not v:
                continue
            params.append(v)
            # print "%s : %s" % ((i + 1), v)

        if check_mol_id < 0 :
            sql = '''INSERT INTO search_moldata (mol_id, mol_name, en_synonyms, zh_synonyms, name_cn, cas_no, 
                                                    formula,mol_weight,exact_mass,smiles,inchi,
                                                    num_atoms,num_bonds,num_residues,sequence,
                                                    num_rings,logp,psa,mr,goods_count) VALUES (
                                                    %s,%s,%s,%s,%s,%s,
                                                    %s,%s,%s,%s,%s,
                                                    %s,%s,%s,%s,
                                                    %s,%s,%s,%s,0
                                                    )'''
            logging.info(u"写入新数据,mol_id:%s!", mol_id)
            # logging.info(sql)
            self.db_dict.insert(sql, *params)
        else:
            sql = '''update search_moldata  set formula=%s,mol_weight=%s,exact_mass=%s,smiles=%s,inchi=%s,
                                                    num_atoms=%s,num_bonds=%s,num_residues=%s,sequence=%s,
                                                    num_rings=%s,logp=%s,psa=%s,mr=%s
                                                    where mol_id=%s
                                                    '''
            logging.info(u"更新数据,mol_id:%s!", mol_id)
            u_params = params[6:]
            u_params.append(mol_id)
            # print u_params
            self.db_dict.execute(sql, *u_params)
        self.delete_data(mol_id)
        # 对mol文件进行相应的格式化
        c = "echo \"%s\" | checkmol -m - 2>&1" % mol
        result = os.popen(c).read()
        # print "molformat>>>" + result
        sql = "insert into search_molstruc values ('%s','%s',0,0)"
        sql = sql % (mol_id, result)
        self.db_dict.insert(sql)
        c = "echo \"%s\" | checkmol -aXbH - 2>&1" % mol
        result = os.popen(c).read()
        # print result
        results = result.split("\n")
        molstat = results[0]
        molfgb = results[1]
        molhfp = results[2]
        if ('unknown' not in molstat) and ('invalid' not in molstat):
            sql = 'insert into search_molstat values (%s,%s)' % (mol_id, molstat)
            # logging.info(u"执行的sql:%s", sql)
            self.db_dict.insert(sql)
            molfgb = molfgb.replace(';', ',')
            sql = 'insert into search_molfgb values (%s,%s)' % (mol_id, molfgb)
            self.db_dict.insert(sql)
            
            molhfp = molhfp.replace(';', ',')
            sql = 'insert into search_molcfp values (%s,%s,%s)'
            cand = "%s$$$$%s" % (mol, self.fpdef)
            cand = cand.replace('$', '\$')
            c = "echo \"%s\" | %s -F - 2>&1" % (cand, dict_conf.MATCHMOL)
            result = os.popen(c).read().replace('\n', '')
            sql = sql % (mol_id, result, molhfp)
            self.db_dict.insert(sql)
        pic_path = str(mol_id)
        while len(pic_path) < 8:
            pic_path = '0' + pic_path
        pic_dir = pic_path[0:4]
        pic_dir = '%s/%s/%s.png' % (pic_dir[0:2], pic_dir[2:4], mol_id)
        pic_fp = dict_conf.worker_bitmapdir + '/' + pic_dir
        if not os.path.exists(pic_fp[0:pic_fp.rfind('/')]):
            os.makedirs(pic_fp[0:pic_fp.rfind('/')])
        self.delete_file(pic_fp)
        # print pic_fp
        # print pic_dir
        c = "echo \"%s\" | %s %s - 2>&1"
        c = c % (mol, dict_conf.MOL2PS, dict_conf.mol2psopt)
        molps = os.popen(c).read()
        c = "echo \"%s\" | %s -q -sDEVICE=bbox -dNOPAUSE -dBATCH  -r300 -g500000x500000 - 2>&1"
        c = c % (molps, dict_conf.GHOSTSCRIPT)
        bb = os.popen(c).read()
        bbs = bb.split('\n')
        bblores = bbs[0].replace('%%BoundingBox:', '').lstrip()
        bbcorner = bblores.split(' ')
        if len(bbcorner) >= 4:
            bbleft = int(bbcorner[0])
            bbbottom = int(bbcorner[1])
            bbright = int(bbcorner[2])
            bbtop = int(bbcorner[3])
            xtotal = (bbright + bbleft) * dict_conf.scalingfactor
            ytotal = (bbtop + bbbottom) * dict_conf.scalingfactor
        if xtotal > 0 and ytotal > 0:
            molps = '%s %s scale\n%s' % (dict_conf.scalingfactor, dict_conf.scalingfactor, molps)
        else:
            xtotal = 99; ytotal = 55
            molps = '''%!PS-Adobe
                    /Helvetica findfont 14 scalefont setfont
                    10 30 moveto
                    (2D structure) show
                    10 15 moveto
                    (not available) show
                    showpage\n''';
        gsopt1 = " -r300 -dGraphicsAlphaBits=4 -dTextAlphaBits=4 -dDEVICEWIDTHPOINTS=%s -dDEVICEHEIGHTPOINTS=%s -sOutputFile=%s"
        gsopt1 = gsopt1 % (xtotal, ytotal, pic_fp)
        c = "echo \"%s\" | %s -q -sDEVICE=pnggray -dNOPAUSE -dBATCH %s - "
        c = c % (molps, dict_conf.GHOSTSCRIPT, gsopt1)
        # print 'command>>' + c
        result = os.popen(c).read()
        # print 'pic_result>>' + result
        c = "file \"%s\" | awk '{print $5, $7}' | awk -F\",\" '{print $1}'"
        c = c % pic_fp
        result = os.popen(c).read().replace('\n', '')
        pic_width = result.split(' ')[0]
        pic_height = result.split(' ')[1]
        status = 1
        # print 'pic_size>>' + result
        sql = "insert into search_pic2d (mol_id,type,status,s_pic,s_width,s_height) values ('%s',1,'%s','%s','%s','%s')"
        sql = sql % (mol_id, status, pic_dir, pic_width, pic_height)
        # print sql
        self.db_dict.insert(sql);
        return mol_id
    
    def check_match(self, cas_no, mol):
        sql = 'select * from search_moldata where cas_no=%s order by mol_id asc'
        rs = self.db_dict.query(sql, cas_no)
        for r in rs:
            return r['mol_id']
        c = "echo \"%s\" | checkmol -axH -" % mol
        # logging.info(c)
        result = os.popen(c).read()
        # logging.info(u"check_mol_result:%s", result)
        chkresult = result.split('\n')
        result1 = chkresult[0]
        result2 = chkresult[1]
        result2 = result2.split(';')[0]
     
        if 'invalid' in result1:
            raise Exception('无效的Mol文件')
        result1 = result1[0: len(result1) - 1]
        result1 = result1.replace(';', ' and ').replace(':', '=').replace('n_', 'stat.n_')
        sql = 'select stat.mol_id,struc.struc from search_molstat as stat, search_molstruc as struc where (%s) and (stat.mol_id=struc.mol_id)'
        sql = sql % (result1)
        # print "[%s]" % result1
        # print "[%s]" % result2
        logging.info(u"执行的sql:%s", sql)
        rs = self.db_dict.query(sql)
        if len(rs) == 0:
            return -1
        
        for r in rs:
            self.delete_file(self.tmp_mol2)
            mol2_writer = open(self.tmp_mol2, 'w')
            mol2_writer.write(r['struc'])
            mol2_writer.close()
            c = "%s -aisxgG %s %s" % (dict_conf.MATCHMOL, self.tmp_mol1, self.tmp_mol2)
            result = os.popen(c).read()
            # 返回相应的molid
            if ':T' in result:
                return r['mol_id']
        pass
    
    def delete_data(self, mol_id):
        sql = 'delete from search_molstruc where mol_id=%s'
        sql = sql % mol_id
        self.db_dict.execute(sql)
        sql = 'delete from search_pic2d where mol_id=%s'
        sql = sql % mol_id
        self.db_dict.execute(sql)
        self.delete_stat_table(mol_id)
    
    def delete_stat_table(self, mol_id):
        sql = 'delete from search_molstat where mol_id=%s'
        sql = sql % mol_id
        self.db_dict.execute(sql)
        sql = 'delete from search_molfgb where mol_id=%s'
        sql = sql % mol_id
        self.db_dict.execute(sql)
        sql = 'delete from search_molcfp where mol_id=%s'
        sql = sql % mol_id
        self.db_dict.execute(sql)
        
    def update_stat_table_db(self):
        counter = 0
        while 1:
            if counter > 10000:
                logging.info(u'循环次数过多,退出')
                break 
            sql = 'select max(mol_id) as mol_id from search_molstat'
            rs = self.db_dict.query(sql)
            mol_id = 0
            for r in rs:
                mol_id = r['mol_id']
                if not mol_id:
                    mol_id = 0
            sql = 'select * from search_molstruc where mol_id>%s limit 1000'
            rs = self.db_dict.query(sql, mol_id)
            logging.info(u"需要修正的加速表的数据量为:%s 起始mol_id:%s", len(rs), mol_id)
            for r in rs:
                self.update_stat_table(r['mol_id'], r['struc'])
            counter += 1
    
    def update_stat_table_fix(self):
        fix_mol_ids = [442574L, 2283521L, 327682L, 327683L, 1589590L, 699262L, 1542145L, 141321L, 43019L, 1779156L, 2465805L, 126991L, 47120L, 2234385L, 327885L, 88083L, 1552405L, 1675286L, 2224151L, 1655457L, 1756847L, 328090L, 481121L, 403488L, 1703228L, 2105379L, 1497124L, 327686L, 327718L, 327719L, 278568L, 1507369L, 45L, 328537L, 1491293L, 1470512L, 2295858L, 88116L, 38965L, 2121782L, 328713L, 1544660L, 1589305L, 1579066L, 1536059L, 1553000L, 26685L, 1640175L, 2119744L, 2078720L, 1536069L, 1536070L, 2101323L, 2101324L, 2248781L, 1654862L, 89101L, 550614L, 928440L, 51282L, 403539L, 327764L, 90197L, 761943L, 127064L, 1411161L, 2097242L, 756751L, 1654877L, 1552478L, 90207L, 127072L, 88080L, 49251L, 1654884L, 88165L, 127078L, 327225L, 88168L, 241770L, 90219L, 88172L, 2107502L, 327357L, 1672365L, 2121842L, 2121843L, 90228L, 1559024L, 2236538L, 247931L, 2119804L, 1544554L, 241791L, 90241L, 327275L, 557188L, 127109L, 2162822L, 127111L, 755849L, 88202L, 1657772L, 2444545L, 88206L, 88207L, 328685L, 2080916L, 1544558L, 2107543L, 2107544L, 755866L, 2123931L, 2234524L, 1544981L, 483486L, 327840L, 88225L, 2078409L, 483492L, 90277L, 127142L, 1712497L, 1028264L, 387834L, 200874L, 1592007L, 143532L, 2238641L, 2238642L, 90291L, 127157L, 88247L, 2482360L, 127161L, 2087961L, 127164L, 127165L, 2458997L, 327872L, 2121922L, 1552548L, 1704135L, 2074526L, 2361719L, 1552589L, 90318L, 327373L, 1421520L, 327890L, 2121939L, 90325L, 90326L, 1640665L, 1640666L, 127195L, 127196L, 327278L, 82142L, 90335L, 328642L, 1775841L, 755939L, 403684L, 88293L, 1785665L, 88295L, 844009L, 26859L, 125965L, 2115822L, 676081L, 88306L, 1562779L, 2248062L, 90358L, 90359L, 2240760L, 195284L, 327932L, 389373L, 2107646L, 194815L, 88320L, 2119938L, 2158851L, 88324L, 485078L, 2248964L, 1734919L, 88328L, 88329L, 327946L, 2406103L, 194828L, 176398L, 88336L, 2138385L, 327043L, 2480404L, 88341L, 2092761L, 194841L, 1026332L, 194846L, 2435360L, 327387L, 403751L, 2289961L, 403506L, 327982L, 1110615L, 1762526L, 194866L, 928819L, 1655459L, 383286L, 88377L, 1552211L, 1487357L, 1429820L, 1315134L, 843829L, 2439488L, 915195L, 2388290L, 2236739L, 2439492L, 2388293L, 88390L, 2193735L, 1672009L, 194891L, 2474316L, 340301L, 1741197L, 2158929L, 483667L, 194900L, 2265016L, 2228566L, 88407L, 328024L, 194906L, 88411L, 2120029L, 88414L, 485093L, 2078779L, 194916L, 60134L, 500071L, 194921L, 1495402L, 1552103L, 1544556L, 2479506L, 88430L, 1786223L, 2457969L, 1654871L, 1641822L, 1628533L, 2299625L, 1448018L, 2470269L, 1471546L, 1217941L, 1672715L, 1554635L, 45444L, 328070L, 328071L, 133513L, 1650279L, 327693L, 721293L, 2120079L, 1789185L, 1662001L, 2130324L, 194965L, 996727L, 2447767L, 328376L, 194969L, 43418L, 2326939L, 2109853L, 88478L, 1752878L, 1761696L, 1539528L, 328101L, 88487L, 327606L, 126023L, 2460402L, 1026478L, 444829L, 2230704L, 328114L, 1552821L, 1722807L, 169617L, 1542900L, 102842L, 88508L, 1510758L, 328127L, 43459L, 484613L, 2269638L, 2453959L, 327695L, 328036L, 88522L, 2105806L, 1543462L, 2226603L, 2122194L, 2253267L, 88532L, 156117L, 301526L, 196078L, 403535L, 328431L, 2116061L, 2185694L, 2185696L, 2073163L, 88547L, 1513956L, 88550L, 327627L, 43497L, 1026541L, 6638L, 195056L, 1569607L, 1657939L, 328022L, 2118135L, 88568L, 1770893L, 2243066L, 1026361L, 1448021L, 88577L, 2461129L, 88579L, 195076L, 328197L, 1589589L, 1744864L, 360968L, 1014281L, 328202L, 88587L, 2328836L, 916703L, 698895L, 328208L, 1762808L, 88597L, 88598L, 88599L, 327313L, 108176L, 328111L, 1634492L, 195101L, 1562775L, 88608L, 1690145L, 1672692L, 1742280L, 385528L, 1546493L, 1113721L, 195113L, 88618L, 756267L, 576135L, 1671261L, 328620L, 268851L, 1649204L, 1544758L, 1542715L, 1562028L, 1186367L, 2165312L, 1690720L, 88642L, 2474563L, 2474565L, 2474566L, 2200135L, 1497674L, 2439755L, 1786444L, 1719223L, 195150L, 1650104L, 127075L, 1675314L, 88661L, 1110614L, 327152L, 760409L, 195002L, 2239072L, 1289659L, 35428L, 2454117L, 195174L, 88679L, 1468545L, 25020L, 2105963L, 1481324L, 1542765L, 88686L, 88688L, 195187L, 576117L, 88694L, 43639L, 924282L, 1543970L, 328316L, 443982L, 88703L, 1654883L, 2118274L, 1545323L, 327216L, 350855L, 1448044L, 88715L, 33421L, 37519L, 195217L, 1649298L, 1704387L, 1424022L, 1551472L, 2382490L, 27292L, 1655456L, 2157217L, 1655458L, 1499235L, 1739117L, 1488487L, 195238L, 327452L, 47786L, 2269639L, 2099884L, 328366L, 1649437L, 949704L, 510643L, 1422004L, 328261L, 195255L, 2339512L, 1432251L, 1671868L, 763338L, 327205L, 1424843L, 35526L, 893639L, 2214600L, 254667L, 195276L, 2390733L, 68302L, 2325199L, 195280L, 180945L, 23252L, 1469141L, 1649366L, 344791L, 482084L, 23258L, 1763154L, 327678L, 2120415L, 1186528L, 694395L, 3878L, 43750L, 1749735L, 755836L, 1679143L, 575954L, 1555183L, 195313L, 195314L, 875252L, 895733L, 843220L, 195322L, 1442943L, 195324L, 1610493L, 1555925L, 1500640L, 2458070L, 928519L, 152328L, 482092L, 1539474L, 2210577L, 2087698L, 1700628L, 88853L, 1678798L, 480026L, 500507L, 195357L, 1433051L, 88868L, 576293L, 88870L, 1025500L, 1671978L, 195372L, 195373L, 88878L, 199133L, 169766L, 2120158L, 1545014L, 148282L, 1662431L, 1666186L, 1491774L, 88897L, 328515L, 2132804L, 97093L, 1542983L, 761996L, 328522L, 125776L, 592722L, 1513955L, 2087701L, 755854L, 246617L, 2163548L, 551322L, 1420129L, 756752L, 1711398L, 195046L, 539496L, 549738L, 539499L, 549740L, 88942L, 1461103L, 43888L, 25457L, 539506L, 88947L, 1555316L, 2452341L, 1788855L, 1471351L, 327272L, 2452347L, 1469308L, 551805L, 207742L, 842901L, 328528L, 2454401L, 2454402L, 2454404L, 207749L, 88967L, 1094536L, 196076L, 1649546L, 721047L, 1471375L, 207762L, 1721235L, 1565461L, 178670L, 328598L, 384040L, 2278298L, 196079L, 1539547L, 893765L, 207777L, 404635L, 88998L, 445352L, 207785L, 126791L, 37804L, 1446829L, 755977L, 2120626L, 2120627L, 328628L, 1657783L, 756664L, 1656308L, 89019L, 1654935L, 327685L, 89023L, 2371603L, 2214849L, 914370L, 1546059L, 1672694L, 406478L, 1461102L, 484298L, 43982L, 327501L, 195064L, 126115L, 327616L, 1552838L, 2137047L, 2137048L, 698532L, 2441381L, 756704L, 37857L, 327248L, 327163L, 2407396L, 58341L, 1795046L, 2135016L, 748524L, 68589L, 2481135L, 125936L, 929619L, 1034409L, 926712L, 25593L, 328701L, 328533L, 1491108L, 2079745L, 327510L, 1555969L, 328712L, 2429961L, 41995L, 25613L, 1664789L, 25615L, 2108432L, 1552216L, 89107L, 574487L, 328196L, 500763L, 1575793L, 2166542L, 2438662L, 89126L, 2440231L, 1033881L, 1657941L, 125995L, 89132L, 1649709L, 68654L, 480304L, 500232L, 631858L, 88243L, 74806L, 1649720L, 1471545L, 927360L, 2407483L, 2200636L, 327178L, 2096190L, 89151L, 126017L, 1741890L, 1662019L, 327887L, 89159L, 89612L, 328372L, 89954L, 2217038L, 328517L, 327171L, 927928L, 1657938L, 56403L, 1657940L, 327182L, 56408L, 56410L, 1657948L, 126045L, 126046L, 926565L, 1668375L, 2194530L, 126051L, 1784007L, 44134L, 2249832L, 126057L, 480363L, 2106479L, 482416L, 2215026L, 327187L, 89205L, 1555988L, 1713385L, 89213L, 89215L, 78976L, 1564787L, 255109L, 1473671L, 1507180L, 2444426L, 1657778L, 1703055L, 327512L, 1733827L, 255124L, 480110L, 122008L, 126106L, 60571L, 2468378L, 926878L, 2471071L, 1461104L, 345041L, 89251L, 327361L, 327311L, 756329L, 126120L, 89257L, 1548550L, 1479853L, 255150L, 928285L, 327732L, 126133L, 2096587L, 327720L, 126136L, 328695L, 482492L, 2127043L, 539846L, 44231L, 484554L, 2467019L, 2467020L, 1434931L, 2374863L, 255185L, 89298L, 2262227L, 74964L, 2266325L, 126166L, 89305L, 327312L, 146651L, 328327L, 2239710L, 196133L, 146657L, 1025251L, 1544977L, 138470L, 126183L, 27881L, 141722L, 89323L, 1650376L, 2138384L, 1551599L, 1650101L, 1733875L, 1033460L, 2478974L, 254505L, 2444536L, 126201L, 1543419L, 2444540L, 2444541L, 2444542L, 194773L, 2444544L, 126209L, 2444546L, 2444547L, 2153733L, 2254086L, 2148225L, 327730L, 1670017L, 763151L, 29969L, 1668372L, 1459417L, 126232L, 1766933L, 1787163L, 439580L, 1446209L, 2221344L, 527649L, 89379L, 1486118L, 126247L, 89384L, 337203L, 339242L, 1672491L, 44334L, 2161968L, 2069809L, 146738L, 89395L, 327315L, 1482208L, 328585L, 44344L, 103737L, 483551L, 1542084L, 1554208L, 1721234L, 330316L, 756619L, 2229572L, 2233669L, 501063L, 89416L, 113993L, 1566028L, 1543501L, 1649998L, 89423L, 103761L, 483143L, 126293L, 720215L, 1627480L, 1033561L, 48477L, 122206L, 328203L, 132450L, 1428835L, 327453L, 122214L, 482664L, 1649446L, 89319L, 763244L, 1672557L, 126319L, 1185218L, 1443185L, 1627506L, 2276724L, 2401760L, 720247L, 892280L, 2123327L, 114045L, 2114942L, 2114943L, 476544L, 1550912L, 1543554L, 90347L, 146821L, 56710L, 327233L, 1649563L, 484749L, 126353L, 327058L, 892307L, 44437L, 2479510L, 2479512L, 1555865L, 2416026L, 1545627L, 327069L, 1674655L, 327072L, 327073L, 327075L, 89509L, 1217959L, 540073L, 89514L, 1543596L, 1656237L, 56750L, 89520L, 892339L, 501172L, 1027552L, 2481592L, 126393L, 843195L, 292284L, 327101L, 1633726L, 439745L, 327106L, 327107L, 327108L, 2383302L, 949703L, 89544L, 196041L, 384458L, 2240759L, 2241435L, 196046L, 327119L, 1304824L, 327276L, 328611L, 89556L, 327125L, 126422L, 574935L, 1551832L, 196057L, 89562L, 2295259L, 763356L, 89565L, 196062L, 1482207L, 126432L, 327137L, 1555938L, 327139L, 196068L, 2108901L, 327142L, 196073L, 196075L, 126444L, 1545709L, 89582L, 2106863L, 196080L, 196081L, 196082L, 402931L, 2237940L, 1656309L, 1656310L, 1656311L, 89592L, 89595L, 327165L, 126465L, 2466005L, 196099L, 196100L, 169478L, 126471L, 89608L, 2110985L, 196106L, 327179L, 2115084L, 196110L, 1553325L, 327184L, 126482L, 196115L, 196116L, 1510933L, 929303L, 126489L, 327194L, 439835L, 2129436L, 327197L, 2102815L, 328414L, 1668390L, 169509L, 1555049L, 114268L, 1556012L, 2434605L, 575760L, 196143L, 2160176L, 2160178L, 328198L, 482869L, 196151L, 1556025L, 327511L, 327263L, 1209916L, 1770339L, 126526L, 89663L, 120385L, 196162L, 1185907L, 196164L, 3653L, 327238L, 327265L, 1545802L, 198583L, 126542L, 89679L, 1542366L, 755640L, 196178L, 327267L, 327252L, 1717846L, 196186L, 1778959L, 89692L, 926415L, 1539680L, 126561L, 1553339L, 1650276L, 1650278L, 2168081L, 403048L, 2121321L, 2143850L, 923239L, 126572L, 349805L, 646481L, 327613L, 169584L, 349809L, 126578L, 1543795L, 297456L, 2414198L, 89719L, 394872L, 126585L, 2133627L, 394877L, 330525L, 278805L, 843392L, 1649707L, 2133635L, 198276L, 89734L, 443699L, 1416843L, 407902L, 327309L, 1771481L, 2104976L, 89368L, 286354L, 1592014L, 928366L, 327318L, 155287L, 558745L, 1739302L, 126620L, 327325L, 1469140L, 327328L, 928949L, 2121378L, 327331L, 2444543L, 1554088L, 720554L, 915116L, 1719981L, 1662006L, 1754346L, 2227888L, 1654892L, 2133683L, 1709783L, 755998L, 1589288L, 1103326L, 1484474L, 762007L, 1565642L, 1732286L, 394944L, 2107073L, 327362L, 1621699L, 419527L, 2115272L, 1554121L, 2238154L, 2316919L, 335565L, 89806L, 327376L, 554708L, 2477006L, 403158L, 71383L, 89816L, 327703L, 843482L, 2473691L, 380638L, 1554305L, 89827L, 2238180L, 89829L, 2447078L, 126695L, 327400L, 2238185L, 2238186L, 2238188L, 2238189L, 894702L, 22255L, 1544634L, 1551193L, 755442L, 1640185L, 268026L, 30459L, 2242300L, 1554173L, 892544L, 1552131L, 169732L, 328302L, 1756935L, 126730L, 1433355L, 327436L, 89870L, 403757L, 762779L, 327442L, 327299L, 328549L, 912686L, 89879L, 126746L, 2481948L, 755485L, 327455L, 3872L, 1556257L, 89890L, 130851L, 87846L, 87847L, 3880L, 755498L, 3883L, 3884L, 87854L, 755506L, 222515L, 2371382L, 327479L, 327480L, 1546042L, 327647L, 1543416L, 484046L, 2178881L, 2125638L, 87879L, 2428745L, 327498L, 2242379L, 1455948L, 1455949L, 1732434L, 2109267L, 1779540L, 327509L, 2482006L, 87895L, 48984L, 88036L, 126811L, 89948L, 2444535L, 87904L, 126817L, 126818L, 1544507L, 337766L, 126823L, 1738600L, 87913L, 1783659L, 755564L, 2066289L, 1339971L, 89971L, 328045L, 87925L, 640888L, 2127738L, 1544831L, 328236L, 87933L, 1542016L, 2129793L, 2363266L, 69507L, 485253L, 1544070L, 403335L, 327803L, 485260L, 2240398L, 2240399L, 948113L, 2271122L, 2107283L, 2107284L, 1544997L, 2432919L, 327663L, 403357L, 626590L, 1543493L, 2362352L, 126295L, 1677219L, 755625L, 2105259L, 1718188L, 90029L, 892846L, 2082461L, 2237647L, 2239816L, 1546162L, 87987L, 87988L, 198581L, 198582L, 2239817L, 87992L, 327934L, 327610L, 2078651L, 1301102L, 49086L, 327669L, 126912L, 403396L, 755653L, 327622L, 1591969L, 327624L, 1432038L, 1554378L, 1631223L, 327628L, 327330L, 126926L, 481553L, 1592313L, 1554393L, 2387279L, 1544156L, 90077L, 126943L, 126944L, 480437L, 1539447L, 126948L, 483665L, 1662952L, 1791996L, 328616L, 1763308L, 327661L, 755709L, 755697L, 327666L, 344403L, 88053L, 88054L, 1743096L, 892923L, 88060L, 2246653L, 327489L, 88063L]
        #fix_mol_ids = [45]
        fix_mol_ids = str(fix_mol_ids).replace('L', '')
        #print fix_mol_ids
        sql = 'select * from search_molstruc where mol_id in (%s)' % fix_mol_ids[1:len(fix_mol_ids) - 1]
        rs = self.db_dict.query(sql)
        for r in rs:
            self.update_stat_table(r['mol_id'], r['struc'])
    
    def update_stat_table(self, mol_id, mol):
        try:
            c = "echo \"%s\" | %s -aXbHs 2>&1" % (mol, dict_conf.CHECKMOL_V2)
            #logging.info(u'执行命令:%s', c)
            result = os.popen(c).read()
            logging.info(u'指令执行的结果为:%s', result)
            results = result.split("\n")
            self.insert_stat_table(mol_id, mol, results)
        except Exception, e:
            logging.error(u"处理mol_id数据时出错:%s", mol_id)
            logging.error(traceback.format_exc())
        
    
    # 新指令更新加速表的数据, 需要和PHP同步修改
    def insert_stat_table(self, mol_id, mol, results):
        molstat = results[0]
        molfgb = results[1]
        molhfp = results[2]
        if ('unknown' in molstat) or ('invalid' in molstat):
            logging.info(u"更新mol_id:%s加速表时，指令返回的结果错误", mol_id)
            return
        logging.info(u"更新mol_id:%s 加速表数据", mol_id)
        self.delete_stat_table(mol_id)
        sql = 'insert into search_molstat values (%s,%s)' % (mol_id, molstat)
        # logging.info(u"执行的sql:%s", sql)
        self.db_dict.insert(sql)
        molfgb = molfgb.replace(';', ',')
        sql = 'insert into search_molfgb values (%s,%s)' % (mol_id, molfgb)
        self.db_dict.insert(sql)
        
        molhfp = molhfp.replace(';', ',')
        sql = 'insert into search_molcfp values (%s,%s,%s)'
        cand = "%s$$$$%s" % (mol, self.fpdef)
        cand = cand.replace('$', '\$')
        c = "echo \"%s\" | %s -Fs 2>&1" % (cand, dict_conf.MATCHMOL_V2)
        result = os.popen(c).read().replace('\n', '')
        print result
        sql = sql % (mol_id, result, molhfp)
        self.db_dict.insert(sql)
    
    def import_table_data(self):
        sql = 'select * from dic_source_data'
        rs = self.db_dict_source.query(sql)
        for r in rs:
            try:
                signal.alarm(10)
                # logging.info(u'处理id:%s cas_no:%s的记录', r['id'], r['cas_no'])
                if not r['cas_no']:
                    logging.info(u'id:%s 记录无cas号', r['id'])
                    continue
                if not self.cu.cas_check(r['cas_no']):
                    logging.info(u'CAS号:%s 校验失败', r['cas_no'])
                    continue
                if not r['inchi'].startswith('InChI='):
                    r['inchi'] = 'InChI=' + r['inchi']
                c = 'echo "%s" | babel -iinchi -ocan'
                c = c % r['inchi']
                result = os.popen(c).read().replace('\r', '').replace('\n', '').strip()
                if not result:
                    logging.info(u"CAS号:%s InChI:%s 格式错误", r['cas_no'], r['inchi'])
                    continue
                data_dict = {'name_en':r['name_en'], 'name_en_alias':r['name_en_alias'], 'name_cn':r['name_cn'], 'name_cn_alias':r['name_cn_alias'], 'cas_no':r['cas_no']}
                c = 'echo "%s" | babel -iinchi -omol --gen2d'
                c = c % r['inchi']
                # logging.info(u'执行生成mol命令:%s', c)
                result = os.popen(c).read()
                # logging.info(u'生成mol的')
                self.write_dic(data_dict, result)
            except Exception, e:
                logging.error(u"处理cas:%s 产品:%s ErrMsg:%s", r['cas_no'], r['name_en'], e)
                logging.error(traceback.format_exc())
            # break
    
    def check_data_exist(self, cas):
        sql = "select * from search_moldata where cas_no='%s'"
        sql = sql % cas
        _rs = self.db_dict.query(sql)
        if len(_rs) > 0:
            logging.info(u'cas:%s 数据已经存在', cas)
            return True
        return False

if __name__ == '__main__':

    logging.basicConfig(format='%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s')
    define("logfile", default="/tmp/data_fix.log", help="NSQ topic")
    define("func_name", default="import_table_data")
    define("sdf_file", default="/home/kulen/Documents/xili_data/xili_2.sdf")
    define("mol_id", default="-1")
    options.parse_command_line()
    logfile = options.logfile
    sdf_file = options.sdf_file
    func_name = options.func_name
    mol_id = int(options.mol_id)
    fmt = '%(asctime)s-%(module)s:%(lineno)d %(levelname)s %(message)s'
    handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=100 * 1024 * 1024, backupCount=10)  # 实例化handler
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt)  # 实例化formatter
    handler.setFormatter(formatter)  # 为handler添加formatter
    logging.getLogger('').addHandler(handler)
    logging.info(u'写入的日志文件为:%s', logfile)
    worker = DictWorker()
    worker.update_stat_table_fix()
    logging.info(u'程序运行完成')
