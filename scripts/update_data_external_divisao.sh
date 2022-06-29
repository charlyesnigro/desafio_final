
HDFS_DIR="/final/divisao"
NOME_PASTA=$1

echo "Efetuando a ingestão na tabela de Divisao"
cd ../dados/${NOME_PASTA}
hdfs dfs -copyFromLocal DIVISAO.csv ${HDFS_DIR}


