use interview;

drop table if exists employee;

CREATE TABLE employee
(
    emp_id INTEGER  NOT NULL,
    name NVARCHAR(20)  NOT NULL,
    salary NVARCHAR(30),
    dept_id INTEGER
);


INSERT INTO employee
(emp_id, name, salary, dept_id)
VALUES(101, 'sohan', '3000', '11'),
(102, 'rohan', '4000', '12'),
(103, 'mohan', '5000', '13'),
(104, 'cat', '3000', '11'),
(105, 'suresh', '4000', '12'),
(109, 'mahesh', '7000', '12'),
(108, 'kamal', '8000', '11');


select * from employee order by dept_id,salary;

select e.emp_id from employee e
join employee b
on e.dept_id=b.dept_id and e.salary =  b.salary and e.emp_id <> b.emp_id
order by e.dept_id,e.salary;


