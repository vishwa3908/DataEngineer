use interview;

drop table if exists family;

create table family 
(
person varchar(5),
type varchar(10),
age int
);

insert into family values ('A1','Adult',54)
,('A2','Adult',53),('A3','Adult',52),('A4','Adult',58),('A5','Adult',54),('C1','Child',20),('C2','Child',19),('C3','Child',22),('C4','Child',15);

select * from family;

with adult as (
select *,row_number() over(order by age desc) as rnk1 from family where type='Adult'),
child as (
select *,row_number() over(order by age) as rnk2 from family where type = 'child')
select a.person as adult,b.person as child from adult a 
left join child b
on a.rnk1=b.rnk2;