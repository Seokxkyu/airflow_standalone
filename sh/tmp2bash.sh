#!/bin/bash

DEL_DT=$1

user="root"

MYSQL_PWD='qwer123' mysql --local-infile=1 -u"$user" <<EOF
USE history_db;
DELETE FROM history_db.cmd_usage WHERE dt='${DEL_DT}';
INSERT INTO cmd_usage
SELECT 
	CASE WHEN dt LIKE '%-%-%'
	THEN STR_TO_DATE(dt, '%Y-%m-%d')
	ELSE STR_TO_DATE('1970-01-01', '%Y-%m-%d')
	END AS dt,
	command,
	CASE WHEN cnt REGEXP '[0-9]+$' 
	THEN CAST(cnt AS UNSIGNED)
	ELSE -1
	END AS cnt
FROM tmp_cmd_usage
WHERE dt = '${DEL_DT}'
;
EOF
