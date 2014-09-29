--业务关联的6张表

use molbase;
select * from search_moldata;
select * from search_molstruc;
select * from search_pic2d;
select * from search_molstat;
select * from search_molfgb;
select * from search_molcfp;

-- 更新这些mol_id的数据
delete from search_moldata where mol_id in (138296,2364314,948191,705373,1632774,1576923,1555123);
delete from search_molstruc where mol_id in (138296,2364314,948191,705373,1632774,1576923,1555123);
delete from search_pic2d where mol_id in (138296,2364314,948191,705373,1632774,1576923,1555123);
delete from search_molstat where mol_id in (138296,2364314,948191,705373,1632774,1576923,1555123);
delete from search_molfgb where mol_id in (138296,2364314,948191,705373,1632774,1576923,1555123);
delete from search_molcfp where mol_id in (138296,2364314,948191,705373,1632774,1576923,1555123);

update search_molstruc set struc=null where mol_id in (1,4);

use molbase;
truncate table search_moldata;
truncate table search_molstruc;
truncate table search_pic2d;
truncate table search_molstat;
truncate table search_molfgb;
truncate table search_molcfp;


insert into fragment_origin (cas,cmpdiupacname,Inchi,Canonical_SMILES) select cas,'',target_inchi,target_smile from etl;
insert into fragment_pubchem (cas,cmpdiupacname,Inchi,Canonical_SMILES) select cas,'',source_inchi,source_smile from etl;

--字典数据导出
mysqldump -h172.16.1.104 -uleon -pmolbase1010 z_dic_molbase search_moldata search_molcfp search_molfgb search_molstat search_molstruc search_pic2d -t -w "mol_id>2391105">/tmp/dict_insertv2.sql
mysqldump -h172.16.1.104 -uleon -pmolbase1010 z_dic_molbase search_moldata search_molcfp search_molfgb search_molstat search_molstruc search_pic2d -t -w "mol_id>2395721">/tmp/dict_insert_pubchem.sql

mol_id>=2487792;

-- 加速表数据导出
alter table search_molstat rename to search_molstat_new;
alter table search_molfgb rename to search_molfgb_new;
alter table search_molcfp rename to search_molcfp_new;
mysqldump -h172.16.1.100 -uleon -pmolbase1010 dict_data_stat search_molstat_new search_molfgb_new search_molcfp_new>/tmp/new_stat.sql
-- 旧表数据备份
alter table search_molstat rename to search_molstat_bak;
alter table search_molfgb rename to search_molfgb_bak;
alter table search_molcfp rename to search_molcfp_bak;
-- 名称还原
alter table search_molstat_new rename to search_molstat;
alter table search_molfgb_new rename to search_molfgb;
alter table search_molcfp_new rename to search_molcfp;


alter table search_moldata rename to search_moldata_bak;
alter table search_molstruc rename to search_molstruc_bak;
alter table search_pic2d rename to search_pic2d_bak;
alter table search_molstat rename to search_molstat_bak;
alter table search_molfgb rename to search_molfgb_bak;
alter table search_molcfp rename to search_molcfp_bak;

alter table search_moldata rename to search_moldata_new;
alter table search_molstruc rename to search_molstruc_new;
alter table search_pic2d rename to search_pic2d_new;
alter table search_molstat rename to search_molstat_new;
alter table search_molfgb rename to search_molfgb_new;
alter table search_molcfp rename to search_molcfp_new;

alter table search_moldata_new rename to search_moldata;
alter table search_molstruc_new rename to search_molstruc;
alter table search_pic2d_new rename to search_pic2d;
alter table search_molstat_new rename to search_molstat;
alter table search_molfgb_new rename to search_molfgb;
alter table search_molcfp_new rename to search_molcfp;


