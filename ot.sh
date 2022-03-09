#!/bin/bash
param_file=${1}
. ${param_file}
. ${MYSQL_PASS_PATH}
start-all.sh
mysql -u${MYSQL_USER} -p${MYSQL_PASS} -e "
create database if not exists project_1;
use project_1;
create table if not exists data_import_staging(
custid INTEGER PRIMARY KEY,
username VARCHAR(100),
quote_count INTEGER,
ip VARCHAR(30),
entry_time VARCHAR(100),
prp_1 INTEGER,
prp_2 INTEGER,
prp_3 INTEGER,
ms INTEGER,
http_type VARCHAR(10), 
purchase_category VARCHAR(100),
total_count INTEGER,
purchase_sub_category VARCHAR(100),
http_info VARCHAR(500),
status_code VARCHAR(10),
table_load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
entry_year INTEGER,
entry_month INTEGER,
entry_day INTEGER
);"
mysql -u${MYSQL_USER} -p${MYSQL_PASS} -e "
use project_1;
create table if not exists data_import_backup(
custid INTEGER PRIMARY KEY,
username VARCHAR(100),
quote_count INTEGER,
ip VARCHAR(30),
entry_time VARCHAR(100),
prp_1 INTEGER,
prp_2 INTEGER,
prp_3 INTEGER,
ms INTEGER,
http_type VARCHAR(10), 
purchase_category VARCHAR(100),
total_count INTEGER,
purchase_sub_category VARCHAR(100),
http_info VARCHAR(500),
status_code VARCHAR(10),
table_load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
entry_year INTEGER,
entry_month INTEGER,
entry_day INTEGER
);"

sqoop job --create inc_project_id -- import \
--connect jdbc:mysql://${IP_ADD}:${PORTNO}/${MYSQL_DB}?useSSL=False \
--username ${MYSQL_USER} --password-file ${MYSQL_P_FILE} \
-m 1 \
--delete-target-dir --target-dir ${HDFS_SQP_PATH} \
--query "select custid, username, quote_count, ip, entry_time, prp_1, prp_2, prp_3, ms, http_type, purchase_category, total_count, purchase_sub_category, http_info, status_code,table_load_date,entry_year,entry_month,entry_day from data_import_staging where \$CONDITIONS"

nohup hive --service metastore &

hive -e "create database if not exists ${HIVE_DB};"

hive -e "use ${HIVE_DB};
create table if not exists ${MGD_TBL}(
custid int,
username string,
quote_count int,
ip string,
entry_time string,
prp_1 int,
prp_2 int,
prp_3 int,
ms int,
http_type string, 
purchase_category string,
total_count int,
purchase_sub_category string,
http_info string,
status_code int,
table_load_date timestamp,
entry_year int,
entry_month int,
entry_day int
)
row format delimited fields terminated by ',';"

hive -e "use ${HIVE_DB};
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.enforce.bucketing=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.compactor.initiator.on=true;
set hive.compactor.worker.threads=1;
create table if not exists ${SCD_TBL}(
custid int,
username string,
quote_count int,
ip string,
entry_time string,
prp_1 int,
prp_2 int,
prp_3 int,
ms int,
http_type string, 
purchase_category string,
total_count int,
purchase_sub_category string,
http_info string,
status_code int,
table_load_date timestamp,
entry_year int,
entry_month int,
entry_day int
)
row format delimited fields terminated by ','
stored as orc
TBLPROPERTIES('transactional'='true');"


hive -e "use ${HIVE_DB};
create external table if not exists ${EXT_TBL} (
custid int,
username string,
quote_count int,
ip string,
entry_time string,
prp_1 int,
prp_2 int,
prp_3 int,
ms int,
http_type string, 
purchase_category string,
total_count int,
purchase_sub_category string,
http_info string,
status_code int,
table_load_date timestamp
)
partitioned by (entry_year int,entry_month int,entry_day int)
row format delimited fields terminated by ',';"