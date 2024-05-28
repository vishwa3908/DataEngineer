-- There are two tables , one table has 5 records and seconfd table has 10 records
-- find minimum and maximun number of records for each joins
use interview;
drop table if exists test_join1;
drop table if exists test_join2;
CREATE TABLE test_join1 (
    id INT PRIMARY KEY,
    name VARCHAR(50)
);
CREATE TABLE test_join2 (
    id INT PRIMARY KEY,
    city VARCHAR(50)
);
INSERT INTO test_join1 (id, name) VALUES
(1, 'Alice'),
(2, 'Bob'),
(3, 'Charlie'),
(4, 'David'),
(5, 'Eve');
INSERT INTO test_join2 (id, city) VALUES
(1, 'New York'),
(2, 'Los Angeles'),
(3, 'Chicago'),
(4, 'Houston'),
(5, 'Phoenix'),
(6, 'Philadelphia'),
(7, 'San Antonio'),
(8, 'San Diego'),
(9, 'Dallas'),
(10, 'San Jose');

select * from test_join1;
select * from test_join2;



-- inner join min - 5
-- left join - 10
-- right join 10

select * from test_join1 a
inner join  test_join2 b
on a.id = b.id;

select * from test_join1 a
left join  test_join2 b
on a.id = b.id;


select * from test_join1 a
right join  test_join2 b
on a.id = b.id;
