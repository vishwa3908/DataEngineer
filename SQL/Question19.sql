use interview;
drop table if exists names;
-- Create table for storing names and emails
CREATE TABLE if not exists names (
    id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL
);

-- Insert records into the 'names' table
INSERT INTO names (first_name, last_name, email) VALUES
    ('Emily', 'Johnson', 'emily.johnson@example.com'),
    ('Benjamin', 'Lee', 'benjamin.lee@example.com'),
    ('Sophia', 'Martinez', 'sophia.martinez@example.com'),
    ('Ethan', 'Thompson', 'ethan.thompson@example.com'),
    ('Olivia', 'Davis', 'olivia.davis@example.com'),
    ('Alexander', 'Rodriguez', 'alexander.rodriguez@example.com'),
    ('Isabella', 'Wilson', 'isabella.wilson@example.com'),
    ('Jacob', 'Garcia', 'jacob.garcia@example.com'),
    ('Ava', 'Hernandez', 'ava.hernandez@example.com'),
    ('Michael', 'Miller', 'michael.miller@example.com'),
    ('Mia', 'Lopez', 'mia.lopez@example.com'),
    ('William', 'Moore', 'william.moore@example.com');


select * from names;
select first_name from names where first_name regexp '^[E]';
select first_name from names where first_name regexp '^[^E]';
select first_name from names where first_name regexp '^[M].*[l]$';
select first_name from names where first_name regexp '^[M].[^l]$';
select first_name from names where first_name regexp '[a]$';
select first_name from names where first_name regexp '[^a]$';