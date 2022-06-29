
HDFS_DIR="/final/vendas"
NOME_PASTA=$1

echo "Efetuando a ingestão na tabela de Vendas"
cd ../dados/${NOME_PASTA}
hdfs dfs -copyFromLocal VENDAS.csv ${HDFS_DIR}

