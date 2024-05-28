/*markdown
### Question
Given a table of purchases by date, calculate the month-over-month 𝐩𝐞𝐫𝐜𝐞𝐧𝐭𝐚𝐠𝐞 𝐜𝐡𝐚𝐧𝐠𝐞 𝐢𝐧 𝐫𝐞𝐯𝐞𝐧𝐮𝐞. The output should include the year-month date (YYYY-MM) and percentage change, rounded to the 2nd decimal point, and sorted from the beginning of the year to the end of the year.
![alt text](image-8.png)
*/

create table amazon_monthly_rev (
id int,
created_at date ,
value int ,
purchase_id int
);

insert into amazon_monthly_rev(id , created_at , value , purchase_id)
values(1,'2019-01-01',172692,43),
(2,'2019-01-05',177194,36),
(3,'2019-02-06',116948,56),
(4,'2019-02-10',162515,29),
(5,'2019-02-14',120741,30),
(6,'2019-03-22',151688,34),
(7,'2019-03-26',1002327,44);

select * from amazon_monthly_rev;

with cte as(
    SELECT 
       CONCAT(YEAR(created_at), '-', LPAD(MONTH(created_at), 2, '0')) as yearmonth,
       sum(value) as total,
       lag(sum(value)) 
       over(order by CONCAT(YEAR(created_at), '-', LPAD(MONTH(created_at), 2, '0')) ) as previous
    FROM amazon_monthly_rev
    group by 1 order by 1
)
select yearmonth as YearMonth,
    round((abs(previous-total)/(previous))*100,2) as precentage_revenue_diff from cte;



