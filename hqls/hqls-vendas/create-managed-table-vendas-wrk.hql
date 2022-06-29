
CREATE DATABASE IF NOT EXISTS ${TARGET_DATABASE};

DROP TABLE IF EXISTS ${TARGET_DATABASE}.${TARGET_TABLE};

CREATE TABLE ${TARGET_DATABASE}.${TARGET_TABLE} (
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

PARTITIONED BY (DT_FOTO STRING)

ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' 
TBLPROPERTIES ( 'orc.compress'='SNAPPY')
;
