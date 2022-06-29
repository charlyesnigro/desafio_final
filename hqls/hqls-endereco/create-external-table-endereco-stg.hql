CREATE DATABASE IF NOT EXISTS ${TARGET_DATABASE};

DROP TABLE IF EXISTS ${TARGET_DATABASE}.${TARGET_TABLE};

CREATE EXTERNAL TABLE IF NOT EXISTS ${TARGET_DATABASE}.${TARGET_TABLE}(
	`Address_Number` integer,
	`City` string,
	`Country` string,
	`Customer_Address_1` string,
	`Customer_Address_2` string,
	`Customer_Address_3` string,
	`Customer_Address_4` string,
	`State` string,
	`Zip_Code` string
)
COMMENT 'Tabela Externa de Endereco'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '${HDFS_DIR}'
tblproperties ('skip.header.line.count'='1', 'store.charset'='UTF-8', 'retrieve.charset'='UTF-8');