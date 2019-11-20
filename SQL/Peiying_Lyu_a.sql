SELECT distinct v.facility_name
FROM violations v
WHERE v.facility_name LIKE "%cafe%" AND 
        v.violation_code = "F030" ;