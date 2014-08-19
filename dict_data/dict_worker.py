# -*- coding: utf-8 -*-
from tornado.options import define, options
import logging
import os, sys
import csv
import re
import traceback
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
        self.db_dict = ConUtil.connect_mysql(dict_conf.MYSQL_DICT)
        self.db_dict_source = ConUtil.connect_mysql(dict_conf.MYSQL_DICT_SOURCE)
        if not os.path.exists(dict_conf.bitmapdir):
            os.makedirs(dict_conf.bitmapdir)
        self.tmp_mol1 = '/tmp/mol1.mol';self.tmp_mol2 = '/tmp/mol2.mol'
        self.i_mol_id = self.get_start_molid()
        self.cu = CasUtil()
        sql = 'select fpdef from moldb_fpdef'
        rs = self.db_dict.query(sql)
        self.fix_cas = ["21040-64-2", "33973-59-0", "162602-04-2", "180164-14-1", "191729-43-8", "211238-60-7", "2259-07-6", "24513-41-5", "2671-32-1", "29748-10-5", "332371-82-1", "3772-56-3", "41653-73-0", "476-70-0", "509-24-0", "5309-35-3", "56377-67-4", "5940-00-1", "62394-00-7", "6619-95-0", "69636-83-5", "74892-45-8", "80434-33-9", "83916-76-1", "87480-84-0", "928712-89-4", "981-15-7", "1033288-92-4", "11027-63-7", "115334-05-9", "119188-47-5", "1246012-24-7", "130838-00-5", "140631-27-2", "15085-71-9", "160047-56-3", "178600-68-5", "189351-15-3", "205115-75-9", "22149-28-6", "24173-71-5", "26187-80-4", "28957-04-2", "32619-42-4", "3621-38-3", "40951-69-7", "4666-84-6", "495-32-9", "52389-15-8", "54835-70-0", "58738-31-1", "61597-55-5", "6519-27-3", "68160-76-9", "73354-15-1", "78510-19-7", "82508-34-7", "86537-66-8", "919120-78-8", "95839-45-5", "1016987-87-3", "108124-75-0", "114531-28-1", "1188281-99-3", "1226-22-8", "128255-16-3", "13895-92-6", "145400-03-9", "1745-36-4", "18749-71-8", "20045-06-1", "218290-59-6", "23526-45-6", "25532-45-0", "27832-84-4", "310433-44-4", "35833-62-6", "390362-53-5", "451478-47-0", "482-91-7", "517883-38-4", "53851-13-1", "57566-47-9", "6054-10-0", "6429-04-5", "6736-85-2", "717901-03-6", "7689-03-4", "81678-46-8", "852638-61-0", "90332-63-1", "943989-68-2", "100432-87-9", "104777-61-9", "113270-98-7", "118024-26-3", "1207181-61-0", "126594-64-7", "135293-13-9", "144223-70-1", "155418-97-6", "167875-39-0", "18463-25-7", "193969-08-3", "214150-74-0", "22841-42-5", "247036-52-8", "268541-26-0", "301530-12-1", "34169-70-5", "38276-59-4", "425680-98-4", "478-61-5", "51020-86-1", "53526-67-3", "56486-94-3", "60008-01-7", "62820-11-5", "66648-45-1", "70051-38-6", "7562-61-0", "80510-06-1", "84567-08-8", "881388-87-0", "931116-24-4", "99891-77-7", "1038922-95-0", "111518-94-6", "1159913-80-0", "119725-20-1", "125-15-5", "132185-84-3", "14260-99-2", "152175-76-3", "1615-94-7", "17946-87-1", "1911-78-0", "210108-91-1", "22338-69-8", "244204-40-8", "2649-64-1", "29376-68-9", "329975-47-5", "3682-02-8", "41137-87-5", "472-30-0", "50656-92-3", "52617-37-5", "55497-79-5", "59014-02-7", "62218-13-7", "6610-55-5", "68799-38-2", "74683-19-5", "79491-71-7", "83324-51-0", "87205-99-0", "925932-08-7", "97399-94-5", "1025023-04-4", "1092555-02-6", "1149388-19-1", "1189801-51-1", "123497-84-7", "13018-10-5", "139561-95-8", "149155-19-1", "157528-81-9", "176520-13-1", "189109-45-3", "20230-41-5", "220328-03-0", "23963-54-4", "259653-54-8", "28594-00-5", "32179-18-3", "35878-41-2", "40456-50-6", "4651-46-1", "486430-93-7", "52117-69-8", "54113-95-0", "5803-62-3", "61186-24-1", "64790-68-7", "67600-94-6", "72715-02-7", "77658-45-8", "82425-45-4", "857672-34-5", "911714-91-5", "954379-68-1", "1014974-98-1", "106518-63-2", "113963-39-6", "1187925-30-9", "121748-11-6", "127-22-0", "136172-60-6", "144881-21-0", "172617-99-1", "185845-89-0", "19888-34-7", "21698-44-2", "23180-57-6", "252742-72-6", "27530-67-2", "30413-84-4", "35214-82-5", "38990-03-3", "442851-27-6", "480-43-3", "51551-29-2", "53755-76-3", "57-24-9", "6018-40-2", "64032-49-1", "6711-69-9", "70411-27-7", "76248-63-0", "81263-98-1", "84847-50-7", "888482-17-5", "93888-59-6", "104055-79-0", "112501-42-5", "1169806-00-1", "120693-49-4", "1254-85-9", "132951-90-7", "142950-86-5", "153229-31-3", "164661-12-5", "18196-13-9", "191729-44-9", "21302-79-4", "227471-20-7", "24513-51-7", "26791-73-1", "29836-27-9", "3368-87-4", "37831-70-2", "41744-39-2", "476682-97-0", "509077-91-2", "531-29-3", "564-14-7", "5945-86-8", "62470-46-6", "66322-34-7", "69768-97-4", "750649-07-1", "80453-44-7", "83945-57-7", "875585-30-1", "928714-06-1", "98665-65-7", "1033747-78-2", "110414-77-2", "115458-73-6", "119309-02-3", "1246012-25-8", "130855-22-0", "141973-41-3", "151103-09-2", "160242-09-1", "178764-92-6", "1897-26-3", "20554-84-1", "221899-21-4", "24211-30-1", "26194-57-0", "28957-08-6", "326594-34-7", "36417-86-4", "40957-99-1", "4674-50-4", "49624-66-0", "52525-35-6", "549-84-8", "5876-17-5", "61671-56-5", "65408-91-5", "6832-60-6", "73891-72-2", "78536-36-4", "82508-36-9", "868409-19-2", "92233-55-1", "959860-49-2", "10178-31-1", "1083195-05-4", "114567-47-4", "1188282-00-9", "122872-03-1", "129212-92-6", "138965-88-5", "146450-83-1", "1747-60-0", "18786-24-8", "20065-99-0", "21852-80-2", "23670-94-2", "256445-66-6", "27994-11-2", "31427-08-4", "35833-69-3", "39388-57-3", "45597-00-0", "48236-96-0", "51804-69-4", "53948-07-5", "57576-29-1", "606125-07-9", "64421-27-8", "67383-32-8", "72021-23-9", "76948-72-6", "81873-08-7", "85287-60-1", "90332-65-3", "94410-22-7", "10048-13-2", "104778-16-7", "113270-99-8", "118169-27-0", "1207181-63-2", "126594-73-8", "135683-73-7", "144424-80-6", "155742-64-6", "16830-15-2", "18465-71-9", "19417-00-6", "21499-24-1", "2308-85-2", "24808-04-6", "269742-39-4", "30220-43-0", "34425-25-7", "38642-49-8", "42830-48-8", "480-33-1", "511-05-7", "5356-56-9", "56755-22-7", "60048-88-6", "631-01-6", "66756-57-8", "70191-83-2", "75775-36-9", "80510-09-4", "84575-10-0", "88546-96-7", "93372-87-3", "99933-32-1", "10391-09-0", "111518-95-7", "116424-69-2", "1202-41-1", "125002-91-7", "132339-37-8", "142628-53-3", "152253-67-3", "1616-93-9", "17948-42-4", "19131-13-6", "22365-47-5", "2447-70-3", "2649-68-5", "29424-96-2", "3301-61-9", "3690-05-9", "41447-15-8", "4728-30-7", "50773-41-6", "52705-93-8", "56218-46-3", "59204-61-4", "62218-23-9", "6610-56-6", "68832-39-3", "74713-15-8", "79995-67-8", "83725-24-0", "874201-05-5", "925932-10-1", "97399-95-6", "10267-31-9", "1092555-03-7", "115028-67-6", "1190225-48-9", "1235126-46-1", "130263-10-4", "139682-36-3", "149252-87-9", "157659-20-6", "177602-14-1", "189264-45-7", "20248-08-2", "220935-39-7", "23971-42-8", "260393-05-3", "28619-41-2", "32207-10-6", "36062-04-1", "405281-76-7", "4657-58-3", "486430-94-8", "522-11-2", "54299-52-4", "58469-06-0", "61217-80-9", "64929-59-5", "67650-47-9", "72755-20-5", "77658-46-9", "82427-77-8", "857897-01-9", "912329-03-4", "95456-43-2", "10163-83-4", "106910-79-6", "113973-31-2", "1187925-31-0", "1220508-29-1", "127-27-5", "136685-37-5", "1449-09-8", "17297-56-2", "186374-63-0", "1990-77-8", "217466-37-0", "23313-21-5", "25330-21-6", "27661-51-4", "30452-60-9", "35286-59-0", "39024-12-9", "4429-63-4", "482-36-0", "51650-59-0", "53755-77-4", "572-30-5", "60337-67-9", "64052-90-0", "6713-27-5", "705973-69-9", "76376-43-7", "81264-00-8", "848669-08-9", "890317-92-7", "93915-36-7", "1042143-83-8", "112523-91-8", "117479-87-5", "1207181-35-8", "1260-05-5", "133360-51-7", "143120-46-1", "154418-16-3", "16503-32-5", "18296-45-2", "191732-72-6", "213329-45-4", "22798-96-5", "24513-57-3", "268214-50-2", "29838-67-3", "337527-10-3", "37921-38-3", "4184-34-3", "477-33-8", "50932-19-9", "531-44-2", "56407-87-5", "596799-30-3", "62498-83-3", "66568-97-6", "69804-59-7", "75513-81-4", "80454-42-8", "84104-80-3", "87562-76-3", "929637-35-4", "98665-66-8", "103476-99-9", "11088-09-8", "115783-35-2", "119400-87-2", "1246012-26-9", "131-03-3", "142203-64-3", "151121-39-0", "16049-28-8", "17884-88-7", "19083-00-2", "207446-90-0", "22255-07-8", "24274-60-0", "26296-50-4", "28978-03-2", "327601-97-8", "3650-43-9", "41059-80-7", "4684-28-0", "4965-99-5", "526-06-7", "550-90-3", "58762-96-2", "61775-19-7", "6587-37-7", "6858-85-1", "74048-71-8", "78916-55-9", "82508-37-0", "869384-82-7", "922522-15-4", "96087-10-4", "102115-79-7", "1083200-79-6", "114586-47-9", "1188282-01-0", "123043-54-9", "129314-37-0", "138965-89-6", "147-85-3", "1748-81-8", "18810-25-8", "20133-19-1", "218780-16-6", "237407-59-9", "256445-68-8", "280565-85-7", "315236-68-1", "35833-70-6", "39729-21-0", "458-37-7", "483-09-0", "51838-83-6", "53948-09-7", "57576-31-5", "60976-49-0", "6474-90-4", "6750-60-3", "72061-63-3", "769928-72-5", "81910-39-6", "853267-91-1", "904665-71-0", "94596-27-7", "1007387-95-2", "104975-02-2", "113557-95-2", "118555-84-3", "121521-90-2", "126724-95-6", "1358-76-5", "14464-90-5", "155759-02-7", "16962-90-6", "185414-25-9", "197307-49-6", "215609-93-1", "23141-25-5", "25127-29-1", "27013-91-8", "30244-37-2", "3484-61-5", "38916-91-5", "4290-13-5", "480-37-5", "511-89-7", "5373-87-5", "56973-51-4", "60102-29-6", "63399-37-1", "66900-93-4", "70206-70-1", "76-78-8", "81241-53-4", "84745-95-9", "88585-86-8", "934739-29-4", "99946-04-0", "1039673-32-9", "111917-59-0", "116498-58-9", "120462-42-2", "125124-68-7", "132342-55-3", "142628-54-4", "15291-75-5", "162059-94-1", "179603-47-5", "191545-24-1", "210537-04-5", "22415-24-3", "24512-62-7", "265644-24-4", "2955-23-9", "33116-33-5", "37239-47-7", "41447-16-9", "473981-11-2", "5085-72-3", "52706-07-7", "56222-03-8", "59219-64-6", "62356-47-2", "66107-60-6", "69251-96-3", "74805-91-7", "802909-72-4", "83864-70-4", "87440-75-3", "927812-23-5", "97399-96-7", "102841-46-3", "1093207-99-8", "115074-93-6", "119188-33-9", "123621-00-1", "130288-60-7", "1399-49-1", "14957-38-1", "158500-59-5", "1782-65-6", "189264-47-9", "2030-53-7", "221289-20-9", "24022-13-7", "260397-58-8", "28645-27-4", "32507-77-0", "36062-05-2", "40672-47-7", "466-09-1", "490-46-0", "5231-60-7", "54377-24-1", "58546-54-6", "61276-17-3", "64998-19-2", "678138-59-5", "72826-63-2", "78012-28-9", "82467-50-3", "85889-15-2", "91269-84-0", "956384-55-7", "1016974-78-9", "107160-24-7", "114027-39-3", "1187951-06-9", "122590-03-8", "127-40-2", "137182-37-7", "14531-47-6", "173429-83-9", "18642-44-9", "19956-53-7", "217650-27-6", "23455-44-9", "25368-01-8", "276870-26-9", "30484-88-9", "354553-35-8", "39024-15-2", "4431-42-9", "482-38-2", "51666-26-3", "53823-03-3", "57296-22-7", "605-14-1", "64121-98-8", "67214-05-5", "70897-14-2", "765316-44-7", "81371-54-2", "848669-09-0", "890928-81-1", "941227-27-6", "104700-97-2", "112652-46-7", "1177-14-6", "1207181-57-4", "126176-79-2", "133453-58-4", "143815-99-0", "155060-48-3", "165338-27-2", "184046-40-0", "19254-69-4", "213329-46-5", "22798-98-7", "246868-97-3", "268214-51-3", "29883-15-6", "33815-57-5", "380487-65-0", "4192-90-9", "477-57-6", "51-34-3", "53319-52-1", "56421-12-6", "5973-06-8", "625096-18-6", "66648-43-9", "69978-82-1", "7559-04-8", "80489-65-2", "84108-17-8", "87592-77-6", "929881-46-9", "99026-99-0", "10351-88-9", "111035-65-5", "1159579-44-8", "1195233-59-0", "1246012-27-0", "13161-75-6", "142279-41-2", "151561-88-5", "160568-14-9", "179388-53-5", "1909-91-7", "207446-92-2", "222629-77-8", "24316-19-6", "26315-07-1", "29028-10-2", "32971-25-8", "36519-42-3", "41137-85-3", "4684-32-6", "501-96-2", "526-87-4", "553-21-9", "58822-47-2", "619326-74-8", "65894-41-9", "6871-44-9", "7432-28-2", "79114-77-5", "828935-47-3", "869799-76-8", "92466-31-4", "97372-53-7", "1021945-29-8", "108723-79-1", "114613-59-1", "1188932-15-1", "1231208-53-9", "129488-34-2", "13956-51-9", "147714-71-4", "175556-08-8", "188300-19-8", "201534-09-0", "21887-01-4", "23811-50-9", "25645-19-6", "28189-90-4", "31575-93-6", "35833-72-8", "3984-73-4", "465-00-9", "483-91-0", "51857-11-5", "54081-48-0", "57672-77-2", "61012-31-5", "64776-96-1", "67560-68-3", "72396-01-1", "77263-06-0", "81910-41-0", "85372-72-1", "90582-44-8", "952485-00-6", "101140-06-1", "105181-06-4", "113558-15-9", "118627-52-4", "121700-27-4", "126737-42-6", "135820-80-3", "144765-80-0", "156368-84-2", "17238-53-8", "1857-30-3", "19833-13-7", "216011-55-1", "23141-27-7", "25161-41-5", "27391-16-8", "30273-62-2", "349534-73-2", "38927-54-7", "4382-33-6", "480-39-7", "5132-66-1", "53734-74-0", "56973-65-0", "60129-63-7", "63399-38-2", "67023-80-7", "70387-38-1", "76035-62-6", "81263-96-9", "84799-31-5", "88642-46-0", "93675-85-5", "103974-74-9", "112047-91-3", "116499-73-1", "120462-45-5", "125164-55-8", "13241-28-6", "142647-71-0", "15291-76-6", "162401-32-3", "17983-82-3", "191547-12-3", "21082-33-7", "22478-65-5", "24512-63-8", "26652-12-0", "295803-03-1", "33228-65-8", "3772-55-2", "41514-64-1", "474-07-7", "509-15-9", "52932-74-8", "56324-54-0", "5928-26-7", "62393-88-8", "66178-02-7", "69586-96-5", "74805-92-8", "80396-57-2", "83905-81-1", "87441-73-4", "928151-78-4", "97938-31-3", "10309-37-2", "109592-60-1", "115321-32-9", "119188-38-4", "124168-04-3", "13040-46-5", "14050-92-1", "15051-81-7", "159623-48-0", "1782-79-2", "189322-69-8", "20460-33-7", "221289-31-2", "2415-24-9", "261351-23-9", "289054-34-8", "32602-81-6", "36151-01-6", "40768-81-8", "466-26-2", "492-14-8", "52358-58-4", "545-24-4", "585534-03-8", "61448-03-1", "6519-26-2", "6812-87-9", "72944-06-0", "78432-78-7", "82508-33-6", "863-76-3", "91653-75-7", "95732-59-5", "1016983-51-9", "1079941-35-7", "114297-20-0", "1188281-98-2", "122590-04-9", "128255-08-3", "13850-16-3", "145382-68-9", "17391-09-2", "18747-42-7", "19956-54-8", "2182-14-1", "23518-30-1", "2545-00-8", "27773-39-3", "305364-91-4", "355143-38-3", "390362-51-3", "449729-89-9", "482-68-8", "517-63-5", "53846-49-4", "57420-46-9", "6052-73-9", "642-17-1", "6730-83-2", "7121-99-5", "76689-98-0", "816456-90-3", "849245-34-7", "89498-91-9", "94285-22-0", "100198-09-2", "104759-35-5", "112693-21-7", "1180-35-4", "1207181-59-6", "126223-29-8", "134476-74-7", "144049-72-9", "155205-65-5", "16566-88-4", "18449-41-7", "193892-33-0", "213905-35-2", "22804-49-5", "24694-80-2", "268214-52-4", "301-19-9", "38230-99-8", "421583-14-4", "477953-07-4", "51005-44-8", "53452-34-9", "56473-67-7", "59979-57-6", "62574-30-5", "66648-44-0", "70-18-8", "75590-33-9", "80508-42-5", "84299-80-9", "87797-84-0", "931114-98-6", "99694-90-3", "103553-98-6", "111441-88-4", "1159579-45-9", "119725-19-8", "124868-11-7", "132185-83-2", "142542-89-0", "152110-17-3", "16100-84-8", "179388-54-6", "190906-61-7", "210108-87-5", "22318-10-1", "24352-51-0", "26488-24-4", "29307-03-7", "32981-86-5", "366450-46-6", "41137-86-4", "471-69-2", "502-69-2", "52611-75-3", "55481-86-2", "58865-88-6", "619326-75-9", "659738-08-6", "6874-98-2", "74560-05-7", "79406-09-0", "831222-78-7", "87085-00-5", "924910-83-8", "97399-93-4", "102227-61-2", "109194-60-7", "114916-05-1", "118916-57-7", "123316-64-3", "130-95-0", "13956-52-0", "148044-47-7", "176519-75-8", "18836-52-7", "20194-41-6", "219649-95-3", "23885-43-0", "2571-22-4", "28254-53-7", "31685-80-0", "358721-33-2", "39945-41-0", "465-16-7", "486-64-6", "52096-50-1", "541-15-1", "578-74-5", "610778-85-3", "647853-82-5", "67567-15-1", "72537-20-3", "77658-39-0", "82003-90-5", "857297-90-6", "90582-47-1", "95416-25-4", "101312-79-2", "1059671-65-6", "1138156-77-0", "1187303-40-7", "121710-02-9", "126882-53-9", "136133-08-9", "144868-43-9", "15648-86-9", "17238-55-0", "185821-32-3", "19865-87-3", "21671-00-1", "2318-78-7", "252333-71-4", "2749-28-2", "30315-04-9", "34981-26-5", "38965-51-4", "4382-34-7", "480-41-1", "51419-51-3", "53734-75-1", "56973-66-1", "60129-64-8", "638203-32-4", "67023-81-8", "70389-88-7", "76045-49-3", "81263-97-0", "84812-00-0", "88721-09-9", "93767-25-0", "104021-39-8", "112137-81-2", "1169805-98-4", "120462-46-6", "125181-21-7", "132586-69-7", "142698-60-0", "15291-77-7"]
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
            # return check_mol_id
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
        pic_fp = dict_conf.bitmapdir + '/' + pic_dir
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
        # logging.info(u"执行的sql:%s", sql)
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
        
    def update_stat_table(self):
        sql = 'select * from search_molstruc'
        rs = self.db_dict.query(sql)
        logging.info(u"需要修正的加速表的数据量为:%s", len(rs))
        for r in rs:
            c = "echo \"%s\" | %s -aXbHs 2>&1" % (r['struc'], dict_conf.CHECKMOL_V2)
            result = os.popen(c).read()
            results = result.split("\n")
            self.insert_stat_table(r['mol_id'], r['struc'], results)
    
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
        c = "echo \"%s\" | %s -F - 2>&1" % (cand, dict_conf.MATCHMOL)
        result = os.popen(c).read().replace('\n', '')
        sql = sql % (mol_id, result, molhfp)
        self.db_dict.insert(sql)
    
    def import_table_data(self):
        sql = 'select * from dic_source_data'
        rs = self.db_dict_source.query(sql)
        for r in rs:
            try:
                # logging.info(u'处理id:%s cas_no:%s的记录', r['id'], r['cas_no'])
                if not r['cas_no']:
                    logging.info(u'id:%s 记录无cas号', r['id'])
                    continue
                if not self.cas_check(r['cas_no']):
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
                result = os.popen(c).read()
                self.write_dic(data_dict, result)
            except Exception, e:
                logging.error(u"处理cas:%s 产品:%s", r['cas_no'], r['name_en'])
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
    define("logfile", default="/tmp/sdf_import.log", help="NSQ topic")
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
    if mol_id > 0:
        worker.i_mol_id = mol_id
    if func_name == 'import_table_data':
        worker.import_table_data()
    if func_name == 'import_sdf':
        worker.import_sdf(sdf_file)
    logging.info(u'程序运行完成')
