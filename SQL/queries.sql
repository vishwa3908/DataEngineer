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

