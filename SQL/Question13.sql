use interview;
drop table if exists orders;

CREATE TABLE orders (
    employee_id INT NOT NULL,
    product VARCHAR(50) NOT NULL,
    CHECK (product IN ('Samsung', 'Xiaomi', 'iPhone'))
);

-- Insert data
INSERT INTO orders (employee_id, product) VALUES (1, 'Samsung');
INSERT INTO orders (employee_id, product) VALUES (1, 'Xiaomi');
INSERT INTO orders (employee_id, product) VALUES (2, 'iPhone');
INSERT INTO orders (employee_id, product) VALUES (2, 'Samsung');
INSERT INTO orders (employee_id, product) VALUES (3, 'Xiaomi');
INSERT INTO orders (employee_id, product) VALUES (3, 'iPhone');

-- Insert data for employee 4 who purchased all three products
INSERT INTO orders (employee_id, product) VALUES (4, 'Samsung');
INSERT INTO orders (employee_id, product) VALUES (4, 'Xiaomi');
INSERT INTO orders (employee_id, product) VALUES (4, 'iPhone');

select * from orders;

SELECT employee_id
FROM orders
WHERE product IN ('Samsung', 'Xiaomi', 'iPhone')
GROUP BY employee_id
HAVING COUNT(DISTINCT product) = 3;




