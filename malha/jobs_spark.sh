

spark-submit \
    --master local[*] \
    --deploy-mode client \
    jobs_processor.py




# Colocando os arquivos do HDFS para o desafio_final
DIR_HDFS="/final/dados_saida"
DIR_UNIX="/input/desafio_final/dados/dados_saida"

# DIM_Cliente
hdfs dfs -get ${DIR_HDFS}/DIM_Cliente.csv/*.csv ${DIR_UNIX}
mv ${DIR_UNIX}/part* ${DIR_UNIX}/DIM_Cliente.csv
# DIM_Localidade
hdfs dfs -get ${DIR_HDFS}/DIM_Localidade.csv/*.csv ${DIR_UNIX}
mv ${DIR_UNIX}/part* ${DIR_UNIX}/DIM_Localidade.csv
# DIM_Tempo
hdfs dfs -get ${DIR_HDFS}/DIM_Tempo.csv/*.csv ${DIR_UNIX}
mv ${DIR_UNIX}/part* ${DIR_UNIX}/DIM_Tempo.csv
# Fato
hdfs dfs -get ${DIR_HDFS}/Fato.csv/*.csv ${DIR_UNIX}
mv ${DIR_UNIX}/part* ${DIR_UNIX}/Fato.csv