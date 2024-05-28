use interview;
drop table if exists call_start_logs;
drop table if exists call_end_logs;


create table call_start_logs
(
phone_number varchar(10),
start_time datetime
);
insert into call_start_logs values
('PN1','2022-01-01 10:20:00'),('PN1','2022-01-01 16:25:00'),('PN2','2022-01-01 12:30:00')
,('PN3','2022-01-02 10:00:00'),('PN3','2022-01-02 12:30:00'),('PN3','2022-01-03 09:20:00');
create table call_end_logs
(
phone_number varchar(10),
end_time datetime
);
insert into call_end_logs values
('PN1','2022-01-01 10:45:00'),('PN1','2022-01-01 17:05:00'),('PN2','2022-01-01 12:55:00')
,('PN3','2022-01-02 10:20:00'),('PN3','2022-01-02 12:50:00'),('PN3','2022-01-03 09:40:00')
;

with cte1 as (
select *,row_number() over(partition by phone_number order by start_time asc)as rnk1 from call_start_logs
),cte2 as (
select *,row_number() over(partition by phone_number order by end_time asc)as rnk2 from call_end_logs
)
select cte1.phone_number,cte1.start_time,cte2.end_time,timestampdiff(minute,cte1.start_time,cte2.end_time) as minutes from cte1
join cte2
on cte1.phone_number = cte2.phone_number and cte1.rnk1 = cte2.rnk2