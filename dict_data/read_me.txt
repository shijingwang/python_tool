程序启动指令
数据表导入指令
python dict_worker.py --func_name=import_table_data --mol_id=2391150
sdf导入指令
python dict_worker.py --func_name=import_sdf --mol_id=2393450  --sdf_file=/tmp/xili_data/xili_1.sdf

起始id
2391105
数据数据更新完成之后的mol_id
2393428

导入sdf
2395200

2395325

2395350

2395616
2395620



数据修正时, 当前数据最大的mol_id
2395719



程序阻塞出错的地方
2014-08-19 10:30:55,098-dict_worker:137 INFO 处理属性:{'cas_no': '849699-55-4'}数据
^CTraceback (most recent call last):
  File "dict_worker.py", line 460, in <module>
    worker.import_sdf(sdf_file)
  File "dict_worker.py", line 93, in import_sdf
    query_mol_id = self.write_dic(v_d, mol)
  File "dict_worker.py", line 166, in write_dic
    result = os.popen(c).read()
KeyboardInterrupt



字典数据修正备忘：
本次修正之前，CAS号重复数据18条
SELECT cas_no,count(*) as number FROM `search_moldata` WHERE mol_id<2391105 group by cas_no having count(*)>1

本次修正之后，CAS号重复数据1053条
SELECT cas_no,count(*) as number FROM `search_moldata` group by cas_no having count(*)>1 order by number desc









