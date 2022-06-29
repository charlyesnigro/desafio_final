from pyspark.sql import SparkSession, dataframe
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql import functions as f
import re


# Criação de Data Frames do Hive

df_clientes = spark.sql("SELECT * FROM desafio_final.TB_CLIENTES")
df_divisao = spark.sql("SELECT * FROM desafio_final.TB_DIVISAO")
df_endereco = spark.sql("SELECT * FROM desafio_final.TB_ENDERECO")
df_regiao = spark.sql("SELECT * FROM desafio_final.TB_REGIAO")
df_vendas = spark.sql("SELECT * FROM desafio_final.TB_VENDAS")


# Visualizando a Df Clientes e Removendo dt_foto.
df_clientes = df_clientes.drop('dt_foto')


# Revomendo as duplicadas da Df Clientes

df_clientes = df_clientes.dropDuplicates(["customerkey"])



# Executei o comando para substituir na coluna LIne of Business os 3 espaços em branco por 'Não Informado'.
df_clientes = df_clientes.withColumn('line_of_business', regexp_replace('line_of_business','   ', 'Não Informado'))


# Visualizando a Df Endereço e Removendo dt_foto.
df_endereco = df_endereco.drop('dt_foto')


# Executando o for ele vai percorrer as colunas a direita e trocar as colunas que estão vazias ou em branco pelo texto 'Não Informado'
for a in df_endereco.columns:
    df_endereco = df_endereco.withColumn(a, rtrim(df_endereco[a]))
    df_endereco = df_endereco.withColumn(a, when(df_endereco[a] == '', "Não Informado")\
                                    .when(df_endereco[a].isNull(), "Não informado")\
                                    .otherwise(df_endereco[a]))

# Removendo as Duplicadas da Df Endereço

df_endereco = df_endereco.dropDuplicates(["address_number"])

# Visualizando a Df Vendas e Removendo dt_foto.

df_vendas = df_vendas.drop('dt_foto')

# Usamos o código abaixo para retirar as linhas Nulas da Df Vendas.

df_vendas = df_vendas.where(col("customerkey").isNotNull())

# Visualizando os tipos de dados da Df Vendas e vimos que alguns colunas estão com o tipo de dados string.

df_vendas.printSchema()

# Como as datas estão como string executamos esse código para transformar no formato de date.

data = ['actual_delivery_date', 'datekey', 'invoice_date', 'promised_delivery_date']

for d in data:
    df_vendas = df_vendas.withColumn(d, to_date(col(d), 'dd/MM/yyyy').alias(d))
  
# Visualizando a Df Divisão e Removendo dt_foto.

df_divisao = df_divisao.drop('dt_foto')

# Visualizando a Df Região e Removendo dt_foto.

df_regiao = df_regiao.drop('dt_foto')

# Criando as Views Clientes, Vendas, Endereço, Divisão, Região

df_clientes.createOrReplaceTempView('vw_clientes')
df_vendas.createOrReplaceTempView('vw_vendas')
df_endereco.createOrReplaceTempView('vw_endereco')
df_divisao.createOrReplaceTempView('vw_divisao')
df_regiao.createOrReplaceTempView('vw_regiao')

# Achando a data minima e máxima das df Vendas

df_vendas.agg(f.min("datekey")).show() 

df_vendas.agg(f.max("datekey")).show()

# Criando a Df Tempo e colocando algumas colunas de data.

df_tempo = spark.sql('''
                SELECT
                datekey,
                DATE_FORMAT(DateKey,"MMMM") AS Mes,
                MONTH(DateKey) AS MesNumero,
                DAY(DateKey) AS Dia,                     
                YEAR(DateKey) as Ano     
                FROM(
                SELECT 
                EXPLODE(SEQUENCE(TO_DATE('2017-01-09'), TO_DATE('2019-03-18'), INTERVAL 1 DAY)) as DateKey)''')


# Criando View Tempo
df_tempo.createOrReplaceTempView('vw_tempo')

# União das Datas Frames Df Clientes, Df Endereço, Df Divisão e Df Região .

df_clientes_STG = spark.sql('''
            SELECT 
            vw_clientes.`customerkey`,             
            vw_clientes.`customer`,              
            vw_clientes.`customer_type`,         
            vw_clientes.`address_number`,       
            vw_divisao.`division_name`,          
            vw_regiao.`region_name`,             
            vw_endereco.`city`,                  
            vw_endereco.`state` ,                
            vw_endereco.`country`               
      
         
            
            
            from vw_clientes
            INNER JOIN vw_divisao
            ON vw_clientes.`division` = vw_divisao.`division`
            INNER JOIN vw_regiao
            ON vw_clientes.`region_code` = vw_regiao.`region_code`
            LEFT JOIN vw_endereco
            ON vw_clientes.`address_number` = vw_endereco.`address_number`
            
''')

df_clientes_STG.createOrReplaceTempView('vw_clientes_STG')

# Verificando as colunas em Branco.

df_clientes_STG.select([f.count(f.when(f.isnull(c), 1)).alias(c) for c in df_clientes_STG.columns]).toPandas()

# Tirando as colunas em Branco para 'Não Informado'.

df_clientes_STG = df_clientes_STG.na.fill('Nao informado')

# União das Datas Frames Df Clientes, Df Endereço, Df Divisão e Df Região e Df Vendas e Df tempo.

df_clientes_vendas = spark.sql('''
            SELECT 
            vw_clientes_STG.`customerkey`,             
            vw_clientes_STG.`customer`,              
            vw_clientes_STG.`customer_type`,         
            vw_clientes_STG.`address_number`,       
            vw_clientes_STG.`division_name`,          
            vw_clientes_STG.`region_name`,             
            vw_clientes_STG.`city`,                  
            vw_clientes_STG.`state` ,                
            vw_clientes_STG.`country`,                                
            vw_vendas.`discount_amount`,         
            vw_vendas.`sales_amount`,            
            vw_vendas.`sales_margin_amount`,     
            vw_vendas.`sales_price`,          
            vw_vendas.`sales_quantity`,       
            vw_tempo.datekey,
            vw_tempo.Mes,
            vw_tempo.MesNumero,
            vw_tempo.Dia,                     
            vw_tempo.Ano
          
          
            from vw_vendas
            INNER JOIN vw_clientes_STG
            ON vw_clientes_STG.`customerkey` = vw_vendas.`customerkey`
            INNER JOIN vw_tempo
            ON vw_vendas.`datekey` = vw_tempo.`datekey`
          
''')

# Criando as Hash's das dimensões.
df_clientes_vendas = df_clientes_vendas.withColumn('SK_Cliente',
                                                                     sha2(concat_ws('', df_clientes_vendas.customerkey, 
                                                                                    df_clientes_vendas.customer,
                                                                                    df_clientes_vendas.customer_type),
                                                                          256))

df_clientes_vendas = df_clientes_vendas.withColumn('SK_Localidade',
                                                                     sha2(concat_ws('', df_clientes_vendas.address_number, 
                                                                                    df_clientes_vendas.division_name,
                                                                                    df_clientes_vendas.city,
                                                                                    df_clientes_vendas.state,
                                                                                    df_clientes_vendas.region_name,
                                                                                    df_clientes_vendas.country),
                                                                          256))

df_clientes_vendas = df_clientes_vendas.withColumn('SK_Tempo',
                                                                     sha2(concat_ws('', df_clientes_vendas.datekey,
                                                                                    df_clientes_vendas.Mes,
                                                                                    df_clientes_vendas.MesNumero,
                                                                                    df_clientes_vendas.Dia,         
                                                                                    df_clientes_vendas.Ano),                                                                    
                                                                       256))


# Criando View clientes_vendas.
df_clientes_vendas.createOrReplaceTempView('vw_clientes_vendas')

# Criando a dimensão Cliente

DIM_Cliente = spark.sql('''
            SELECT 
            DISTINCT SK_Cliente,
            customerkey,
            customer,
            Customer_Type 
            FROM vw_clientes_vendas
''')

# Criando a dimensão localidade.

DIM_Localidade = spark.sql('''
            SELECT 
            DISTINCT SK_Localidade,
            Address_Number,
            Division_Name,
            Region_Name,
            City,
            State,
            Country
            FROM vw_clientes_vendas
''')

# Criando a dimensão Tempo.

DIM_Tempo = spark.sql('''
            SELECT 
            DISTINCT SK_Tempo,
            DateKey,
            Dia, 
            Mes,
            MesNumero,
            Ano
            FROM vw_clientes_vendas
''')

# Criando a tabela Fato

Fato = spark.sql('''
            SELECT 
            SK_Cliente,
            SK_Localidade,
            SK_Tempo,
            discount_amount,
            sales_amount,
            sales_margin_amount,
            sales_price,
            sales_quantity
            
            FROM vw_clientes_vendas
''')

# Salvando as dimensões e fato na dados_saida.

DIM_Cliente.coalesce(1).write.mode('overwrite').options(header='True', delimiter=';').csv("/final/dados_saida/DIM_Cliente.csv")

DIM_Tempo.coalesce(1).write.mode('overwrite').options(header='True', delimiter=';').csv("/final/dados_saida/DIM_Tempo.csv")

DIM_Localidade.coalesce(1).write.mode('overwrite').options(header='True', delimiter=';').csv("/final/dados_saida/DIM_Localidade.csv")

Fato.coalesce(1).write.mode('overwrite').options(header='True', delimiter=';').csv("/final/dados_saida/Fato.csv")