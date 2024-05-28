/*markdown
### Question 
𝑭𝒊𝒏𝒅 𝒕𝒉𝒆 𝒔𝒕𝒂𝒓𝒕 𝒂𝒏𝒅 𝒆𝒏𝒅 𝒍𝒐𝒄𝒂𝒕𝒊𝒐𝒏 𝒇𝒐𝒓 𝒆𝒂𝒄𝒉 𝒄𝒖𝒔𝒕𝒐𝒎𝒆𝒓
![alt text](image-6.png)

*/

-- CREATE TABLE journey (
--     customer VARCHAR(50),
--     start_location VARCHAR(50),
--     end_location VARCHAR(50)
-- );
-- INSERT INTO journey (customer, start_location, end_location) VALUES
-- ('c1', 'New York', 'Lima'),
-- ('c1', 'London', 'New York'),
-- ('c1', 'Lima', 'Sao Paulo'),
-- ('c1', 'Sao Paulo', 'New Delhi'),
-- ('c2', 'Mumbai', 'Hyderabad'),
-- ('c2', 'Surat', 'Pune'),
-- ('c2', 'Hyderabad', 'Surat'),
-- ('c3', 'Kochi', 'Kurnool'),
-- ('c3', 'Lucknow', 'Agra'),
-- ('c3', 'Agra', 'Jaipur'),
-- ('c3', 'Jaipur', 'Kochi');





/*markdown


*/

/*markdown

*/

select * from journey;
WITH cte AS (
    SELECT customer, start_location as loc FROM journey
    UNION ALL 
    SELECT customer, end_location as loc FROM journey
    
),cte_2 as 
(
    SELECT customer,loc FROM cte
    group by customer,loc having count(loc)=1
)SELECT 
    journey.customer, 
    MAX(CASE WHEN start_location = loc THEN start_location END) AS start,
    MAX(CASE WHEN end_location = loc THEN end_location END) AS end
FROM journey 
JOIN cte_2 
    ON journey.customer = cte_2.customer AND (journey.start_location = cte_2.loc OR journey.end_location = cte_2.loc)
GROUP BY customer;

