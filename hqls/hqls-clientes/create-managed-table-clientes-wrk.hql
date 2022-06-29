
CREATE DATABASE IF NOT EXISTS ${TARGET_DATABASE};

DROP TABLE IF EXISTS ${TARGET_DATABASE}.${TARGET_TABLE};

CREATE TABLE ${TARGET_DATABASE}.${TARGET_TABLE} (
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

PARTITIONED BY (DT_FOTO STRING)

ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' 
TBLPROPERTIES ( 'orc.compress'='SNAPPY')
;
