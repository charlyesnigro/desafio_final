#!/bin/bash

HDFS_DIR="/final/clientes"
TARGET_DATABASE="desafio_final"
TARGET_TABLE_EXTERNAL="TB_CLIENTES_STG"
TARGET_TABLE="TB_CLIENTES"
DT_FOTO="$(date --date="-1 day" "+%Y-%m-%d")"


# Script eh usado para efetuar a criação das tabelas

# CRIACAO DA TABELA EXTERNA
beeline -u jdbc:hive2://localhost:10000 \
 --hivevar TARGET_DATABASE="${TARGET_DATABASE}"\
 --hivevar HDFS_DIR="${HDFS_DIR}"\
 --hivevar TARGET_TABLE="${TARGET_TABLE_EXTERNAL}"\
 -f ../../hqls/hqls-clientes/create-external-table-clientes-stg.hql 

# CRIACAO DA TABELA WORKED
beeline -u jdbc:hive2://localhost:10000 \
 --hivevar TARGET_DATABASE="${TARGET_DATABASE}"\
 --hivevar TARGET_TABLE="${TARGET_TABLE}" \
 -f ../../hqls/hqls-clientes/create-managed-table-clientes-wrk.hql 


