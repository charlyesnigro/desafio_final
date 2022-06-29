CREATE DATABASE IF NOT EXISTS ${TARGET_DATABASE};

DROP TABLE IF EXISTS ${TARGET_DATABASE}.${TARGET_TABLE};

CREATE EXTERNAL TABLE IF NOT EXISTS ${TARGET_DATABASE}.${TARGET_TABLE}(
	`Address_Number` integer,
	`Business_Family` string,
	`Business_Unit` integer,
	`Customer` string,
	`CustomerKey` integer,
	`Customer_Type` string,
	`Division` integer,
	`Line_of_Business` string,
	`Phone` string,
	`Region_Code` integer,
	`Regional_Sales_Mgr` string,
	`Search_Type` string
)
COMMENT 'Tabela Externa de Clientes'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '${HDFS_DIR}'
tblproperties ('skip.header.line.count'='1', 'store.charset'='UTF-8', 'retrieve.charset'='UTF-8');