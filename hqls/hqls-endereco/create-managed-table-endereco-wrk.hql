
CREATE DATABASE IF NOT EXISTS ${TARGET_DATABASE};

DROP TABLE IF EXISTS ${TARGET_DATABASE}.${TARGET_TABLE};

CREATE TABLE ${TARGET_DATABASE}.${TARGET_TABLE} (
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

PARTITIONED BY (DT_FOTO STRING)

ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' 
TBLPROPERTIES ( 'orc.compress'='SNAPPY')
;
