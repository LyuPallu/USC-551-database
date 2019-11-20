SELECT distinct v.facility_name
FROM violations as v
    join
    (SELECT facility_id, count(facility_id) AS COUNT
    FROM violations 
    GROUP BY facility_id
    ORDER BY COUNT DESC
    LIMIT 1) as v2
    on v.facility_id = v2.facility_id
ORDER BY  v.facility_name;
