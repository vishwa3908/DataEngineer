use interview;
drop table if exists company_revenue;


create table company_revenue 
(
company varchar(100),
year int,
revenue int
);

insert into company_revenue values 
('ABC1',2000,100),('ABC1',2001,110),('ABC1',2002,120),('ABC2',2000,100),('ABC2',2001,90),('ABC2',2002,120)
,('ABC3',2000,500),('ABC3',2001,400),('ABC3',2002,600),('ABC3',2003,800);


with cte as (
select *,coalesce(lag(revenue) over(partition by company order by year asc),0) as previous ,
revenue- coalesce(lag(revenue) over(partition by company order by year asc),0) as diff,
count(1) over(partition by company) as cnt  from company_revenue
)
select * from (select company,cnt, count(*) as inc from cte 
where diff >0 
group by company) tmp where cnt = inc;




with cte  as (select *,coalesce(lag(revenue) over(partition by company order by year asc),0) as previous ,
revenue- coalesce(lag(revenue) over(partition by company order by year asc),0) as diff from company_revenue )
select distinct(company) from cte  where company not in (select company from cte where diff < 0);