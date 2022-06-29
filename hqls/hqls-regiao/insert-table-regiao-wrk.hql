
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE
    ${TARGET_DATABASE}.${TARGET_TABLE}

PARTITION(DT_FOTO) 
SELECT
	Region_Code,
	Region_Name,
	'${DT_FOTO}' as DT_FOTO
FROM ${STAGE_DATABASE}.${STAGE_TABLE}
;
