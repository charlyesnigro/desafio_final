#!/bin/bash

HDFS_DIR="/final/regiao"
TARGET_DATABASE="desafio_final"
TARGET_TABLE_EXTERNAL="TB_REGIAO_STG"
TARGET_TABLE="TB_REGIAO"
DT_FOTO="$(date --date="-1 day" "+%Y-%m-%d")"


# Script eh usado para efetuar a criação das tabelas

# CRIACAO DA TABELA EXTERNA
beeline -u jdbc:hive2://localhost:10000 \
 --hivevar TARGET_DATABASE="${TARGET_DATABASE}"\
 --hivevar HDFS_DIR="${HDFS_DIR}"\
 --hivevar TARGET_TABLE="${TARGET_TABLE_EXTERNAL}"\
 -f ../../hqls/hqls-regiao/create-external-table-regiao-stg.hql 

# CRIACAO DA TABELA WORKED
beeline -u jdbc:hive2://localhost:10000 \
 --hivevar TARGET_DATABASE="${TARGET_DATABASE}"\
 --hivevar TARGET_TABLE="${TARGET_TABLE}" \
 -f ../../hqls/hqls-regiao/create-managed-table-regiao-wrk.hql 


