SELECT distinct facility_name
FROM inspections
WHERE score = (SELECT MAX(score) FROM inspections);
