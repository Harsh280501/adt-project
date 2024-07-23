-- Create the employees table with additional columns

DROP TABLE employees;
use EmployeeDB;


CREATE TABLE employees (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(255),
    position NVARCHAR(255),
    salary DECIMAL(10, 2),
    hire_date DATE,
    department NVARCHAR(255),
    email NVARCHAR(255),
    phone_number NVARCHAR(20),
    address NVARCHAR(255)
);

-- Insert 5 random records
DECLARE @i INT = 1;
DECLARE @name NVARCHAR(255);
DECLARE @department NVARCHAR(255);
DECLARE @position NVARCHAR(255);
DECLARE @salary DECIMAL(10, 2);
DECLARE @hire_date DATE;
DECLARE @rand INT;

WHILE @i <= 5
BEGIN
    SET @name = CONCAT('Name', @i);
    SET @position = CASE 
                       WHEN RAND() < 0.25 THEN 'Software Engineer'
                       WHEN RAND() < 0.5 THEN 'Project Manager'
                       WHEN RAND() < 0.75 THEN 'Data Scientist'
                       ELSE 'UX Designer'
                    END;
    SET @salary = ROUND((RAND() * 50000) + 50000, 2);
    SET @hire_date = DATEADD(DAY, -1 * RAND() * 3650, GETDATE());
    SET @department = CASE 
                         WHEN RAND() < 0.2 THEN 'HR'
                         WHEN RAND() < 0.4 THEN 'Finance'
                         WHEN RAND() < 0.6 THEN 'IT'
                         WHEN RAND() < 0.8 THEN 'Marketing'
                         ELSE 'Sales'
                      END;
    SET @rand = FLOOR(RAND() * 10000);
    INSERT INTO employees (name, position, salary, hire_date, department, email, phone_number, address)
    VALUES (
        @name,
        @position,
        @salary,
        @hire_date,
        @department,
        CONCAT(@name, '@example.com'),
        CONCAT('555-123-', RIGHT('0000' + CAST(@rand AS NVARCHAR(4)), 4)),
        CONCAT(@rand, ' ', CASE WHEN RAND() < 0.5 THEN 'Main St' ELSE 'Elm St' END, ', City', @rand % 100, ', State', @rand % 50)
    );

    SET @i = @i + 1;
END;

-- Verify the data
SELECT * FROM employees;



