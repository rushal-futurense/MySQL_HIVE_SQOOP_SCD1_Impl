#!/bin/bash\
param_file=$1
. ${param_file}
. ${MYSQL_PASS_PATH}
dataset=$2
start-all.sh

mysql -u${MYSQL_USER} -p${MYSQL_PASS} -e "
set GLOBAL local_infile=1;
quit;"
mysql --local-infile=1 -u${MYSQL_USER} -p${MYSQL_PASS} -e "
use project_1;
truncate data_import_staging;
LOAD DATA LOCAL INFILE '${MYSQL_DATA_IM}${dataset}' 
INTO TABLE data_import_staging 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
(custid, username, quote_count, ip, entry_time, prp_1, prp_2, prp_3, ms, http_type, purchase_category, total_count, purchase_sub_category, http_info, status_code,@entry_year,@entry_month,@entry_day)
SET entry_year = DATE_FORMAT(STR_TO_DATE(entry_time,'%d/%b/%Y'),'%Y'),entry_month = DATE_FORMAT(STR_TO_DATE(entry_time,'%d/%b/%Y'),'%m'),entry_day = DATE_FORMAT(STR_TO_DATE(entry_time,'%d/%b/%Y'),'%d');"

mysql --local-infile=1 -u${MYSQL_USER} -p${MYSQL_PASS} -e "insert into data_import_backup select * from data_import_staging";

sqoop job --exec inc_project_id

nohup hive --service metastore &

hive -e "
use project_1;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.enforce.bucketing=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.compactor.initiator.on=true;
set hive.compactor.worker.threads=1;
set hive.auto.convert.join=false;
truncate table data_load_staging;
load data inpath '/user/saif/HFS/Output/mysql/*' into table data_load_staging;
insert into table data_load_ext partition (entry_year,entry_month,entry_day) select custid,username,quote_count,ip,entry_time,prp_1,prp_2,prp_3,ms,http_type,purchase_category,total_count,purchase_sub_category, http_info,status_code,table_load_date,entry_year,entry_month,entry_day from data_load_staging;
MERGE INTO data_load_scd1
USING data_load_staging as y
ON data_load_scd1.custid = y.custid
WHEN MATCHED AND (data_load_scd1.status_code != y.status_code)
THEN
    UPDATE SET status_code = y.status_code
WHEN NOT MATCHED THEN
    INSERT VALUES(y.custid, y.username, y.quote_count, y.ip, y.entry_time, y.prp_1, y.prp_2, y.prp_3, y.ms, y.http_type, y.purchase_category, y.total_count, y.purchase_sub_category, y.http_info, y.status_code, y.table_load_date, y.entry_year, y.entry_month, y.entry_day);
INSERT OVERWRITE LOCAL DIRECTORY '/home/saif/LFS/cohort_c9/project_1/test_p/datasets/hive_export/'ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM data_load_scd1;"

mysql --local-infile=1 -u${MYSQL_USER} -p${MYSQL_PASS} -e "
use project_1;
truncate data_hive_exp;
LOAD DATA LOCAL INFILE '/home/saif/LFS/cohort_c9/project_1/test_p/datasets/hive_export/*' 
INTO TABLE data_hive_exp  
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'"

mysql --local-infile=1 -u${MYSQL_USER} -p${MYSQL_PASS} -e "
use project_1; 
with e as (select count(distinct custid) as cnt from data_import_backup),
d as (select count(distinct custid) as cnt from data_hive_exp)
select abs(e.cnt-d.cnt) as ReconValue
from e,d;
