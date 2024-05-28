use interview;

drop table if exists products;

CREATE TABLE IF NOT EXISTS products (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category_name VARCHAR(255) NOT NULL,
    price DECIMAL(10, 2),
    sales INT,
    rating DECIMAL(3, 2)
);


-- Insert sample data into products
INSERT INTO products (product_name, category_name, price, sales, rating) VALUES 
('Smartphone', 'Electronics', 699.99, 150, 4.5),
('Laptop', 'Electronics', 999.99, 100, 4.7),
('Headphones', 'Electronics', 199.99, 200, 4.3),
('Tablet', 'Electronics', 299.99, 50, 4.4),
('Fiction Book', 'Books', 15.99, 300, 4.8),
('Non-Fiction Book', 'Books', 20.99, 250, 4.6),
('Textbook', 'Books', 59.99, 80, 4.2),
('T-Shirt', 'Clothing', 19.99, 500, 4.5),
('Jeans', 'Clothing', 49.99, 300, 4.4),
('Jacket', 'Clothing', 89.99, 150, 4.7);


select * from products;


select * from (select * , price*sales as total_sales,dense_rank() over(partition by category_name order by price*sales desc) as rnk from products)temp where rnk in (1,2);