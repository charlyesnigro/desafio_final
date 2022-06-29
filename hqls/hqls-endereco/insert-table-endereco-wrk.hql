
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE
    ${TARGET_DATABASE}.${TARGET_TABLE}

PARTITION(DT_FOTO) 
SELECT
	Address_Number,
	City,
	Country,
	Customer_Address_1,
	Customer_Address_2,
	Customer_Address_3,
	Customer_Address_4,
	State,
	Zip_Code,
	'${DT_FOTO}' as DT_FOTO
FROM ${STAGE_DATABASE}.${STAGE_TABLE}
;
