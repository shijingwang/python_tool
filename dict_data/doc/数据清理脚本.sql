--业务关联的6张表

use molbase;
select * from search_moldata;
select * from search_molstruc;
select * from search_pic2d;
select * from search_molstat;
select * from search_molfgb;
select * from search_molcfp;

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