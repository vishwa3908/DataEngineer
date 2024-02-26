/*markdown
### Question
Delete Duplicate records from table
*/

-- create table if not exists duplicate
-- (
--     id int,
--     name varchar(50)
-- );
-- insert into duplicate values (1,'Alice'),
-- (2,'Bob'),
-- (1,'Alice'),
-- (2,'Bob'),
-- (1,'Alice'),
-- (2,'Bob'),
-- (1,'Alice');

select * from duplicate;

delete from duplicate where select *,row_number() over(partition by id,name order by id,name) as rank1 from duplicate;



