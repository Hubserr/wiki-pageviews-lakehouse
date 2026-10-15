with dates as (

    select explode(sequence(to_date('2026-01-01'),to_date('2027-12-31'),interval 1 day)) as dateday

)

select

    cast(date_format(dateday,'yyyyMMdd') as int) as date_key,
    dateday as date_day,
    year(dateday) as year_number,
    quarter(dateday) as quarter_number,
    month(dateday) as month_number, 
    date_format(dateday,'MMMM') as month_name,
    dayofmonth(dateday) as day_of_month,
    weekday(dateday) + 1  as day_of_week,
    date_format(dateday,'EEEE') as day_name,
    weekday(dateday) >=5 as is_weekend

 from dates