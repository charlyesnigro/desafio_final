
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE
    ${TARGET_DATABASE}.${TARGET_TABLE}

PARTITION(DT_FOTO) 
SELECT
	Address_Number,
	Business_Family,
	Business_Unit,
	Customer,
	CustomerKey,
	Customer_Type,
	Division,
	Line_of_Business,
	Phone,
	Region_Code,
	Regional_Sales_Mgr,
	Search_Type,
	'${DT_FOTO}' as DT_FOTO
FROM ${STAGE_DATABASE}.${STAGE_TABLE}
;
