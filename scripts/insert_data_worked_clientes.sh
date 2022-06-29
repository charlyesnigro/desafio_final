

TARGET_DATABASE="desafio_final"
TARGET_TABLE="TB_CLIENTES"
STAGE_TABLE="TB_CLIENTES_STG"
STAGE_DATABASE="desafio_final"

DT_FOTO="$(date "+%Y-%m-%d")"

beeline -u jdbc:hive2://localhost:10000 \
 --hivevar TARGET_DATABASE="${TARGET_DATABASE}"\
 --hivevar TARGET_TABLE="${TARGET_TABLE}" \
 --hivevar STAGE_TABLE="${STAGE_TABLE}" \
 --hivevar STAGE_DATABASE="${STAGE_DATABASE}" \
 --hivevar DT_FOTO="${DT_FOTO}" \
 -f ../hqls/hqls-clientes/insert-table-clientes-wrk.hql 