select distinct i.facility_name
from inspections i 
where i.facility_id not in 
                (select v.facility_id
                 from violations v)
order by i.facility_name;