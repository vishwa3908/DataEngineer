use interview;

drop table if exists users;
 
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100)
);

INSERT INTO users (id, name) VALUES
(1, 'Alice'),
(2, 'Bob'),
(3, 'Charlie'),
(4, 'David'),
(5, 'Eve'),
(6, 'Frank'),
(7, 'Grace');


select * from users;

select *, 
case 
when id%2=0 then (select name from users u where u.id=users.id-1)
when id%2<>0 and id not in  (select count(*) from users) then (select name from users u where u.id=users.id+1) 
else name end as name
from users;
