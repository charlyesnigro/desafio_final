

echo "Deletando Pasta de saida no HDFS"
hdfs dfs -rm -r /final/dados_saida



hdfs dfs -rm -r /final/clientes



echo "Deletanto tabela de CLIENTES"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TB_CLIENTES;"
echo "Deletanto tabela de CLIENTES_EXTERNA"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TB_CLIENTES_STG;"


hdfs dfs -rm -r /final/vendas


echo "Deletanto tabela de VENDAS"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TB_VENDAS;"
echo "Deletanto tabela de VENDAS_EXTERNA"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TB_VENDAS_STG;"




hdfs dfs -rm -r /final/endereco


echo "Deletanto tabela de ENDERECO"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TB_ENDERECO;"
echo "Deletanto tabela de ENDERECO_EXTERNA"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TB_ENDERECO_STG;"



hdfs dfs -rm -r /final/regiao


echo "Deletanto tabela de REGIAO"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TB_REGIAO;"
echo "Deletanto tabela de REGIAO_EXTERNA"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TB_REGIAO_STG;"



hdfs dfs -rm -r /final/divisao


echo "Deletanto tabela de DIVISAO"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TB_DIVISAO;"
echo "Deletanto tabela de DIVISAO_EXTERNA"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TB_DIVISAO_STG;"

echo "Deletanto banco do desafio_final"
beeline -u jdbc:hive2://localhost:10000 -e "DROP DATABASE IF EXISTS desafio_final;"

