use interview;
drop table if exists main;
drop table if exists dummy;
create table if not exists main(
id int,
name varchar(30));

insert into main values(1,'sam'),(2,'paul');

select * from main;

create table dummy as select * from main where 1=0;

select * from dummy;