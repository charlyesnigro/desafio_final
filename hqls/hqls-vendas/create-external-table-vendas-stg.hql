CREATE DATABASE IF NOT EXISTS ${TARGET_DATABASE};

DROP TABLE IF EXISTS ${TARGET_DATABASE}.${TARGET_TABLE};

CREATE EXTERNAL TABLE IF NOT EXISTS ${TARGET_DATABASE}.${TARGET_TABLE}(
	`Actual_Delivery_Date` string,
	`CustomerKey` integer,
	`DateKey` string,
	`Discount_Amount` string,
	`Invoice_Date` string,
	`Invoice_Number` integer,
	`Item_Class` string,
	`Item_Number` string,
	`Item` string,
	`Line_Number` integer,
	`List_Price` string,
	`Order_Number` integer,
	`Promised_Delivery_Date` string,
	`Sales_Amount` string,
	`Sales_Amount_Based_on_List_Price` string,
	`Sales_Cost_Amount` string,
	`Sales_Margin_Amount` string,
	`Sales_Price` string,
	`Sales_Quantity` integer,
	`Sales_Rep` integer,
	`U/M` string
)
COMMENT 'Tabela Externa de Vendas'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '${HDFS_DIR}'
tblproperties ('skip.header.line.count'='1', 'store.charset'='UTF-8', 'retrieve.charset'='UTF-8');