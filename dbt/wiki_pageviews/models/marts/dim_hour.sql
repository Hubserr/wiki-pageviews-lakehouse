with hours as (

    select explode(sequence(0,23)) as hour

)

select
    hour as hour_key,
    hour,
    concat(lpad(hour,2,'0'),':00') as hour_label_utc,
    case 
        when hour <6  then 'night'
        when hour <12 then 'morning'
        when hour <18 then 'afternoon'
        else 'evening'
    end  as part_of_day_utc
from hours