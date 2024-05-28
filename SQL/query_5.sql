/*markdown
### Question 
![alt text](image-7.png)
*/

-- create table source(id int, name varchar(5));

-- create table target(id int, name varchar(5));

-- insert into source values(1,'A'),(2,'B'),(3,'C'),(4,'D');

-- insert into target values(1,'A'),(2,'B'),(4,'X'),(5,'F');

with cte as (select *,'source' as table_name from source
union all
select *,'target' as table_name from target)
select id 




