

#### FAZENDO A INGESTÃO NA TABELA CLIENTES ##########
# Ler Arquivo de Clientes enviar para o hdfs
echo "efetuando a ingestão..."
bash ../scripts/update_data_external_clientes.sh dados_entrada
bash ../scripts/insert_data_worked_clientes.sh



# verificar a partição
echo "Listando as Partições"
beeline -u jdbc:hive2://localhost:10000 -e "SHOW PARTITIONS desafio_final.TB_CLIENTES;"

# descrever a tabela
echo "Descrevendo a Tabela Clientes"
beeline -u jdbc:hive2://localhost:10000 -e "DESCRIBE desafio_final.TB_CLIENTES;"

# count na tabela
echo "Quantidade de Registros da Tabela"
beeline -u jdbc:hive2://localhost:10000 -e "SELECT COUNT(*) FROM desafio_final.TB_CLIENTES;"

# mostrar as 10 primeiras linhas da tabela
echo "Mostrando Apenas os 10 primeiros"
beeline -u jdbc:hive2://localhost:10000 -e "SELECT * FROM desafio_final.TB_CLIENTES LIMIT 10;"

#### FAZENDO A INGESTÃO NA TABELA VENDAS ##########
# Ler Arquivo de Vendas enviar para o hdfs
echo "efetuando a ingestão..."
bash ../scripts/update_data_external_vendas.sh dados_entrada
bash ../scripts/insert_data_worked_vendas.sh

# verificar a partição
echo "Listando as Partições"
beeline -u jdbc:hive2://localhost:10000 -e "SHOW PARTITIONS desafio_final.TB_VENDAS;"

# descrever a tabela
echo "Descrevendo a Tabela Vendas"
beeline -u jdbc:hive2://localhost:10000 -e "DESCRIBE desafio_final.TB_VENDAS;"

# count na tabela
echo "Quantidade de Registros da Tabela"
beeline -u jdbc:hive2://localhost:10000 -e "SELECT COUNT(*) FROM desafio_final.TB_VENDAS;"

# mostrar as 10 primeiras linhas da tabela
echo "Mostrando Apenas os 10 primeiros"
beeline -u jdbc:hive2://localhost:10000 -e "SELECT * FROM desafio_final.TB_VENDAS LIMIT 10;"

#### FAZENDO A INGESTÃO NA TABELA ENDERECO ##########
# Ler Arquivo de Endereço enviar para o hdfs
echo "efetuando a ingestão..."
bash ../scripts/update_data_external_endereco.sh dados_entrada
bash ../scripts/insert_data_worked_endereco.sh

# verificar a partição
echo "Listando as Partições"
beeline -u jdbc:hive2://localhost:10000 -e "SHOW PARTITIONS desafio_final.TB_ENDERECO;"

# descrever a tabela
echo "Descrevendo a Tabela Endereco"
beeline -u jdbc:hive2://localhost:10000 -e "DESCRIBE desafio_final.TB_ENDERECO;"

# count na tabela
echo "Quantidade de Registros da Tabela"
beeline -u jdbc:hive2://localhost:10000 -e "SELECT COUNT(*) FROM desafio_final.TB_ENDERECO;"

# mostrar as 10 primeiras linhas da tabela
echo "Mostrando Apenas os 10 primeiros"
beeline -u jdbc:hive2://localhost:10000 -e "SELECT * FROM desafio_final.TB_ENDERECO LIMIT 10;"

#### FAZENDO A INGESTÃO NA TABELA REGIAO ##########
# Ler Arquivo de Região enviar para o hdfs
echo "efetuando a ingestão..."
bash ../scripts/update_data_external_regiao.sh dados_entrada
bash ../scripts/insert_data_worked_regiao.sh

# verificar a partição
echo "Listando as Partições"
beeline -u jdbc:hive2://localhost:10000 -e "SHOW PARTITIONS desafio_final.TB_REGIAO;"

# descrever a tabela
echo "Descrevendo a Tabela Regiao"
beeline -u jdbc:hive2://localhost:10000 -e "DESCRIBE desafio_final.TB_REGIAO;"

# count na tabela
echo "Quantidade de Registros da Tabela"
beeline -u jdbc:hive2://localhost:10000 -e "SELECT COUNT(*) FROM desafio_final.TB_REGIAO;"

# mostrar as 10 primeiras linhas da tabela
echo "Mostrando Apenas os 10 primeiros"
beeline -u jdbc:hive2://localhost:10000 -e "SELECT * FROM desafio_final.TB_REGIAO LIMIT 10;"

#### FAZENDO A INGESTÃO NA TABELA DIVISAO ##########
# Ler Arquivo de Divisão enviar para o hdfs
echo "efetuando a ingestão..."
bash ../scripts/update_data_external_divisao.sh dados_entrada
bash ../scripts/insert_data_worked_divisao.sh

# verificar a partição
echo "Listando as Partições"
beeline -u jdbc:hive2://localhost:10000 -e "SHOW PARTITIONS desafio_final.TB_DIVISAO;"

# descrever a tabela
echo "Descrevendo a Tabela Divisao"
beeline -u jdbc:hive2://localhost:10000 -e "DESCRIBE desafio_final.TB_DIVISAO;"

# count na tabela
echo "Quantidade de Registros da Tabela"
beeline -u jdbc:hive2://localhost:10000 -e "SELECT COUNT(*) FROM desafio_final.TB_DIVISAO;"

# mostrar as 10 primeiras linhas da tabela
echo "Mostrando Apenas os 10 primeiros"
beeline -u jdbc:hive2://localhost:10000 -e "SELECT * FROM desafio_final.TB_DIVISAO LIMIT 10;"