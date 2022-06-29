#!/bin/bash

HDFS_DIR="/final/divisao"
TARGET_DATABASE="desafio_final"
TARGET_TABLE_EXTERNAL="TB_DIVISAO_STG"
TARGET_TABLE="TB_DIVISAO"
DT_FOTO="$(date --date="-1 day" "+%Y-%m-%d")"


# Script eh usado para efetuar a criação das tabelas

# CRIACAO DA TABELA EXTERNA
beeline -u jdbc:hive2://localhost:10000 \
 --hivevar TARGET_DATABASE="${TARGET_DATABASE}"\
 --hivevar HDFS_DIR="${HDFS_DIR}"\
 --hivevar TARGET_TABLE="${TARGET_TABLE_EXTERNAL}"\
 -f ../../hqls/hqls-divisao/create-external-table-divisao-stg.hql 

# CRIACAO DA TABELA WORKED
beeline -u jdbc:hive2://localhost:10000 \
 --hivevar TARGET_DATABASE="${TARGET_DATABASE}"\
 --hivevar TARGET_TABLE="${TARGET_TABLE}" \
 -f ../../hqls/hqls-divisao/create-managed-table-divisao-wrk.hql 


