#!/bin/bash

HDFS_DIR="/final/vendas"
TARGET_DATABASE="desafio_final"
TARGET_TABLE_EXTERNAL="TB_VENDAS_STG"
TARGET_TABLE="TB_VENDAS"
DT_FOTO="$(date --date="-1 day" "+%Y-%m-%d")"


# Script eh usado para efetuar a criação das tabelas

# CRIACAO DA TABELA EXTERNA
beeline -u jdbc:hive2://localhost:10000 \
 --hivevar TARGET_DATABASE="${TARGET_DATABASE}"\
 --hivevar HDFS_DIR="${HDFS_DIR}"\
 --hivevar TARGET_TABLE="${TARGET_TABLE_EXTERNAL}"\
 -f ../../hqls/hqls-vendas/create-external-table-vendas-stg.hql 

# CRIACAO DA TABELA WORKED
beeline -u jdbc:hive2://localhost:10000 \
 --hivevar TARGET_DATABASE="${TARGET_DATABASE}"\
 --hivevar TARGET_TABLE="${TARGET_TABLE}" \
 -f ../../hqls/hqls-vendas/create-managed-table-vendas-wrk.hql 


