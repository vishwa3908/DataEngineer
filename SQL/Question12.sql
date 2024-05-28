use interview;

drop table if exists test_join1;
drop table if exists test_join2;

create table test_join1 (
id int,
val int);

create table test_join2 (
id int,
val int);

insert into test_join1 
values(1,1),(2,1),(3,1),(4,null);

insert into test_join2 
values(1,2),(2,2),(3,2),(4,null),(5,2);

select * from test_join1 a
inner join test_join2 b
on a.id=b.id;

select * from test_join1 a
left join test_join2 b
on a.id=b.id;
select * from test_join1 a
right join test_join2 b
on a.id=b.id;

select * from test_join1;
select * from test_join2;