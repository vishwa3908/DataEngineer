/*markdown
# Interview Questions
*/

/*markdown
###   Question 1

Challenge : Find the origin and the destination of each customer.
Note : There can be more than 1 stops for the same customer journey.

Create DataFrame Code : 


flights_data = [(1,'Flight1' , 'Delhi' , 'Hyderabad'),
 (1,'Flight2' , 'Hyderabad' , 'Kochi'),
 (1,'Flight3' , 'Kochi' , 'Mangalore'),
 (2,'Flight1' , 'Mumbai' , 'Ayodhya'),
 (2,'Flight2' , 'Ayodhya' , 'Gorakhpur')
 ]

_schema = "cust_id int, flight_id string , origin string , destination string"

*/

with temp1 as
(
    select *,row_number() over(partition by cust_id order by flight_id)as rank1,
    row_number() over(partition by cust_id order by flight_id desc) as rank2
    from flights
),temp2 as 
(
    select cust_id,origin from temp1 where rank1 = 1
),temp3 as 
(
    select cust_id,destination from temp1 where rank2=1
)
select a.cust_id,a.origin,b.destination
from temp2 a
inner join temp3 b
on a.cust_id=b.cust_id;

with temp1 as
(
    select 
        *, 
        row_number() over(partition by cust_id order by flight_id) as rank1,
        row_number() over(partition by cust_id order by flight_id desc) as rank2
    from flights
),
combined_temp as 
(
    select 
        cust_id, 
        origin, 
        destination,
        rank1,
        rank2
    from temp1 
    where rank1 = 1 or rank2 = 1
)
select a.cust_id,a.origin,b.destination from combined_temp a
inner join combined_temp b
on a.cust_id = b.cust_id and (a.rank1 = 1 and b.rank2=1);

-- another approach

with temp1 as
(
    select 
        *, 
        row_number() over(partition by cust_id order by flight_id) as rank1,
        row_number() over(partition by cust_id order by flight_id desc) as rank2
    from flights
),
combined_temp as 
(
    select 
        cust_id, 
        max(case when rank1 = 1 then origin end)  as origin_a,
        max(case when rank2 = 1 then destination end)  as destination_b
    from temp1
    group by cust_id
)
select * from combined_temp;


/*markdown
## Question 2
how can you add one or multiple column before or after a specific column in mysql


*/


-- ALTER TABLE your_table
-- ADD COLUMN new_column1 datatype1 AFTER existing_column1,
-- ADD COLUMN new_column2 datatype2 AFTER existing_column2,
-- ...;



-- select * from employee;
select * from flights order by cust_id asc;

/*markdown
#### find all name of employees staring with vowels
*/

select name from employee where lower(name) regexp '^[aeiou]';

/*markdown
#### find all names of employee not starting with vowels
*/

select name from employee where lower(name) regexp '^[^aeiou]';

/*markdown
#### find all names of employee not ending with vowels
*/

select name from employee where lower(name) regexp '[^aeiou]$';

/*markdown
#### find all names of employee ending with vowels
*/

select name from employee where lower(name) regexp '[aeiou]$';

/*markdown

#### find names of employee whose name starts with vowel but not end with vowel

*/

select name from employee where lower(name) regexp '^[aeiou].*[^aeiou]$';

/*markdown
#### find names of employee not staring with vowel but ending with vowels;
*/

select name from employee where lower(name) regexp  '[^aeiou][aeiou]$';

/*markdown
#### find names of employee whose name starts with vowel or ends with vowel
#### find names of employee whose name starts and ends with vowel
*/

select name from employee where lower(name) regexp '^[aeiou]|[aeiou]$';
select name from employee where lower(name) regexp '^[aeiou].*[aeiou]$';

/*markdown
#### find number of records for each joins
condition1

table structure = id -> 1,1,2,2,2,3

condition2
table structure = id -> 1,2,3,4
*/

select * from test_join;
select * from test_join_1;






select * from test_join t 
inner join test_join_1 t1 
on t.id=t1.id;
select * from test_join_1 t 
inner join test_join t1 
on t.id=t1.id;

select * from test_join t 
left join test_join_1 t1 
on t.id=t1.id;
select * from test_join_1 t 
right join test_join t1 
on t.id=t1.id;


/*markdown
#### 𝑪𝒉𝒂𝒍𝒍𝒆𝒏𝒈𝒆 : Get the name of the 𝐡𝐢𝐠𝐡𝐞𝐬𝐭 𝐬𝐚𝐥𝐚𝐫𝐲 𝐚𝐧𝐝 𝐥𝐨𝐰𝐞𝐬𝐭 𝐬𝐚𝐥𝐚𝐫𝐲 employee name in 𝐞𝐚𝐜𝐡 𝐝𝐞𝐩𝐚𝐫𝐭𝐦𝐞𝐧𝐭. If the salary is same then return the name of the employee whose name comes first in 𝐥𝐞𝐱𝐢𝐜𝐨𝐠𝐫𝐚𝐩𝐡𝐢𝐜𝐚𝐥 𝐨𝐫𝐝𝐞𝐫.
![Alt text](image-1.png)

*/

-- first approach

with cte  as
(
    select *,dense_rank() over(partition by department order by salary asc, name asc)as rank1,
    dense_rank() over(partition by department order by salary desc, name asc)as rank2
    from employee
)
,cte1 as
(
    select id,department,name as min_salary  from cte where rank1=1
)
,cte2 as
(
    select id,department,name as max_salary  from cte where rank2=1
)
select a.id,a.department,a.min_salary,b.max_salary from cte1 a
inner join cte2 b
on a.department=b.department;

-- 2nd approach

WITH cte AS (
    SELECT
        id,
        department,
        name,
        DENSE_RANK() OVER (PARTITION BY department ORDER BY salary ASC, name ASC) AS min_rank,
        DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC, name ASC) AS max_rank
    FROM
        employee
)
SELECT
    department,
    MAX(CASE WHEN min_rank = 1 THEN name END) AS min_salary,
    MAX(CASE WHEN max_rank = 1 THEN name END) AS max_salary
FROM
    cte
GROUP BY
    department;



/*markdown

## Employees Earning More Than Their Managers

 https://thecleverprogrammer.com/2023/05/24/employees-earning-more-than-their-managers-using-sql/

*/

-- CREATE TABLE if not exists emp (
--   id INT PRIMARY KEY,
--   name VARCHAR(255),
--   salary INT,
--   managerId INT
-- );

-- INSERT INTO emp (id, name, salary, managerId) VALUES
--   (1, 'Rahul', 5000, 3),
--   (2, 'Rohit', 8000, 4),
--   (3, 'Suresh', 6000, NULL),
--   (4, 'Manish', 9000, 3);

select * from emp;

select a.id,a.name,a.salary,a.managerId
from emp a
join emp b
on a.managerId=b.id
where a.salary > b.salary;

/*markdown
## Write a solution to delete all duplicate emails, keeping only one unique email with the smallest id.
*/

drop TABLE if exists person;
CREATE TABLE Person (
    id INT PRIMARY KEY,
    email VARCHAR(255)
);

INSERT INTO Person (id, email) VALUES
    (1, 'john@example.com'),
    (2, 'bob@example.com'),
    (3, 'john@example.com'),
    (4, 'alice@example.com'),
    (5, 'charlie@example.com'),
    (6, 'david@example.com'),
    (7, 'alice@example.com'),
    (8, 'bob@example.com'),
    (9, 'emily@example.com'),
    (10, 'john@example.com'),
    (11, 'frank@example.com'),
    (12, 'helen@example.com'),
    (13, 'david@example.com'),
    (14, 'bob@example.com'),
    (15, 'emily@example.com');




select * from person;

-- DELETE p
-- FROM Person p
-- JOIN (
--     SELECT id, email, ROW_NUMBER() OVER (PARTITION BY email ORDER BY email ASC) AS roww
--     FROM Person
-- ) temp ON p.id = temp.id
-- WHERE temp.roww != 1;


DELETE FROM Person
WHERE id NOT IN (
    SELECT id
    FROM (
        SELECT id, ROW_NUMBER() OVER (PARTITION BY email ORDER BY id) AS row_num
        FROM Person
    ) AS temp
    WHERE row_num = 1
);




SELECT * from person;

/*markdown
## Concept of Recursive CTE
*/

/*markdown
WITH RECURSIVE cte_name AS
(
    SELECT query (Non Recursive query  or the Base query)
    UNION ALL
    SELECT query (Recursive query  using cte_name [with a termination condition])
)
select * from cte_name;
*/

create database if not exists relations;
use relations;
show tables;
CREATE TABLE if not exists employees (
    employee_id INT PRIMARY KEY,
    employee_name VARCHAR(255) NOT NULL,
    manager_id INT
    -- Add other columns as needed
);

-- INSERT INTO employees (employee_id, employee_name, manager_id) VALUES
--     (1, 'John Doe', NULL),        -- CEO, top-level manager
--     (2, 'Alice Smith', 1),        -- Manager reporting to CEO
--     (3, 'Bob Johnson', 2),        -- Employee reporting to Alice
--     (4, 'Charlie Brown', 3),     -- Employee reporting to Bob
--     (5, 'Diana Williams', 2),     -- Another employee reporting to Alice
--     (6, 'Eva Davis', 5);          -- Employee reporting to Diana




select * from relations.employees;

/*markdown
Question 1: find all heiarchy as column and also all manager nbame concatenated as column if no managet null
*/

use relations;
select * from employees order by manager_id asc;


WITH RECURSIVE EmployeeLevels AS (
    SELECT
        employee_id,
        manager_id,
        0 AS level
    FROM
        employees
    WHERE
        manager_id IS NULL -- Assuming the top-level manager has a NULL manager_id

    UNION ALL

    SELECT
        e.employee_id,
        e.manager_id,
        el.level + 1
    FROM
        employees e
    JOIN
        EmployeeLevels el ON e.manager_id = el.employee_id
)
SELECT
    employee_id,
    manager_id,
    level
FROM
    EmployeeLevels;




WITH RECURSIVE EmployeeLevels AS (
    SELECT
        employee_id,
        manager_id,
        employee_name,
        employee_name AS manager_names,
        0 AS level
    FROM
        employees
    WHERE
        manager_id IS NULL

    UNION ALL

    SELECT
        e.employee_id,
        e.manager_id,
        e.employee_name,
        CONCAT(el.manager_names, ' > ', e.employee_name),
        el.level + 1
    FROM
        employees e
    JOIN
        EmployeeLevels el ON e.manager_id = el.employee_id
)
SELECT
    employee_id,
    employee_name,
    manager_names,
    level
FROM
    EmployeeLevels;













/*markdown
## Question

Write a query that'll identify returning active users. A returning active user is a user that has made a second purchase within 7 days of any other of their purchases. Output a list of user_ids of these returning active users.
![alt text](image-2.png)


<!-- create table amazon_interview (
user_id int,
item varchar(50),
created_at date,
revenue bigint
);

insert into amazon_interview(user_id , item , created_at, revenue)
values(100, 'bread', '07-03-2020', 410),(100, 'banana' ,'13-03-2020' ,175),
(100, 'banana','29-03-2020', 599),
(101, 'milk', '01-03-2020', 449),
(101, 'milk', '26-03-2020', 740),
(114, 'banana', '10-03-2020', 200),
(114, 'biscuit', '16-03-2020', 300); -->
*/

-- create table amazon_interview (
-- user_id int,
-- item varchar(50),
-- created_at date,
-- revenue bigint
-- );

-- INSERT INTO amazon_interview (user_id, item, created_at, revenue)
-- VALUES
-- (100, 'bread', '2020-03-07', 410),
-- (100, 'banana', '2020-03-13', 175),
-- (100, 'banana', '2020-03-29', 599),
-- (101, 'milk', '2020-03-01', 449),
-- (101, 'milk', '2020-03-26', 740),
-- (114, 'banana', '2020-03-10', 200),
-- (114, 'biscuit', '2020-03-16', 300);



-- select * from amazon_interview;

-- with cte as
-- (
--     select * from(SELECT *,lag(created_at) over(partition by user_id order by created_at) as previous_day from amazon_interview)temp where previous_day is not null
-- )select distinct(user_id) from cte where TIMESTAMPDIFF(DAY, previous_day, created_at) <=7;

/*markdown
#### Question :- Calculate the % Marks for each student. Each subject is of 100 marks. Create a result column by following the below condition 
![alt text](image-3.png)
*/


select * from student;
select * from marks;
with cte as
(
    
)

/*markdown
### Question 
Get the name of the 𝐡𝐢𝐠𝐡𝐞𝐬𝐭 𝐬𝐚𝐥𝐚𝐫𝐲 𝐚𝐧𝐝 𝐥𝐨𝐰𝐞𝐬𝐭 𝐬𝐚𝐥𝐚𝐫𝐲 employee name in 𝐞𝐚𝐜𝐡 𝐝𝐞𝐩𝐚𝐫𝐭𝐦𝐞𝐧𝐭. If the salary is same then return the name of the employee whose name comes first in 𝐥𝐞𝐱𝐢𝐜𝐨𝐠𝐫𝐚𝐩𝐡𝐢𝐜𝐚𝐥 𝐨𝐫𝐝𝐞𝐫.
*/

select * from employee;
with cte as
(
    select name,department,salary,
    dense_rank() over(partition by department order by salary desc,name desc) as rank1,
    dense_rank() over(partition by department order by salary ,name ) as rank2
    from employee
)
select department,
max(case when rank1 = 1 then name end)as max_salary,
    max(case when rank2 = 1 then name end)as min_salary
from cte
group by department;



/*markdown
### Question 

Retrieve information about consecutive login streaks for employee who have logged in for at least two consecutive days.

For each employee provide the emp_id , the number of consecutive days logged in ,the start_date of the streak and end_date of the streak.

![alt text](image-4.png)
*/

-- CREATE TABLE if not exists pwc_attandance_log (
--  emp_id INT,
--  log_date DATE,
--  flag CHAR
-- );
-- insert into pwc_attandance_log(emp_id,log_date,flag) values
-- (101, '2024-01-02', 'N'),
-- (101, '2024-01-03', 'Y'),
-- (101, '2024-01-04', 'N'),
-- (101, '2024-01-07', 'Y'),
-- (102, '2024-01-01', 'N'),
-- (102, '2024-01-02', 'Y'),
-- (102, '2024-01-03', 'Y'),
-- (102, '2024-01-04', 'N'),
-- (102, '2024-01-05', 'Y'),
-- (102, '2024-01-06', 'Y'),
-- (102, '2024-01-07', 'Y'),
-- (103, '2024-01-01', 'N'),
-- (103, '2024-01-04', 'N'),
-- (103, '2024-01-05', 'Y'),
-- (103, '2024-01-06', 'Y'),
-- (103, '2024-01-07', 'N');

-- select * from pwc_attandance_log;
WITH cte AS (
    SELECT *,
           DATEDIFF(log_date, previous_login) AS diff
    FROM (
        SELECT *,
               LAG(log_date) OVER(PARTITION BY emp_id ORDER BY log_date) AS previous_login
        FROM pwc_attandance_log
    ) AS temp
    WHERE previous_login IS NOT NULL
      AND flag <> 'N'
      AND DATEDIFF(log_date, previous_login) = 1
)
SELECT *
FROM cte;


/*markdown
### Question 6
Write a solution to swap the seat id of every two consecutive students. If the number of students is odd, the id of the last student is not swapped.

![alt text](image-5.png)
*/

select * from class;
select *,
case when count(Student)%2=0 then 'odd'
else 'even' end as 'check'
from class
group by Student;

/*markdown

*/