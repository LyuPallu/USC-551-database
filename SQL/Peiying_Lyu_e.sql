SELECT grade, AVG(score)
FROM inspections
where grade != ' '
GROUP BY grade;
