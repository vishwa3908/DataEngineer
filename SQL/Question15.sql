use interview;
drop table if exists hospital;

create table hospital ( emp_id int
, action varchar(10)
, time datetime);

insert into hospital values ('1', 'in', '2019-12-22 09:00:00');
insert into hospital values ('1', 'out', '2019-12-22 09:15:00');
insert into hospital values ('2', 'in', '2019-12-22 09:00:00');
insert into hospital values ('2', 'out', '2019-12-22 09:15:00');
insert into hospital values ('2', 'in', '2019-12-22 09:30:00');
insert into hospital values ('3', 'out', '2019-12-22 09:00:00');
insert into hospital values ('3', 'in', '2019-12-22 09:15:00');
insert into hospital values ('3', 'out', '2019-12-22 09:30:00');
insert into hospital values ('3', 'in', '2019-12-22 09:45:00');
insert into hospital values ('4', 'in', '2019-12-22 09:45:00');
insert into hospital values ('5', 'out', '2019-12-22 09:40:00');

select * from hospital;


-- with cte as (select *,case when action = 'out' then 0
-- when action = 'in' then 1 end as total ,row_number() over(partition by emp_id order by time desc) as rnk from hospital)
-- select * from cte where rnk=1 and total =1;

with cte as (select *,case when action='in' then 1
when action = 'out' then 0 end  as total
,row_number() over(partition by emp_id order by time desc)as rnk from hospital)
select * from cte where total =1 and rnk=1;
