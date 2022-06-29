

echo "Criando Pastas no HDFS"
hdfs dfs -mkdir -p /final/clientes
hdfs dfs -mkdir -p /final/vendas
hdfs dfs -mkdir -p /final/endereco
hdfs dfs -mkdir -p /final/divisao
hdfs dfs -mkdir -p /final/regiao



echo "Criando as Tabelas no desafio final"
cd scripts/create_tables

bash create_tables_clientes.sh
bash create_tables_vendas.sh
bash create_tables_endereco.sh
bash create_tables_divisao.sh
bash create_tables_regiao.sh




echo "Criando Pasta de saida no HDFS"
hdfs dfs -mkdir -p /final/dados_saida
