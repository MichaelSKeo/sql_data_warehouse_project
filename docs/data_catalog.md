## Data Dictionary for Gold Layer

### Overview 
#### The Gold layer is the busines-level data presentation, structured to support analytical and reporting use cases. It consists of dimension tabels and fact tables for specific business metrics. 
-------------------------------------

1. gold.dim_customers:
    - ***Purpose:*** store customer details enriched with demographic and geographic data.
    - ***Columns:***

| Column Name    | Data Type    | Description                                                                        |
| -----------    | ---------    | -----------------------------------------------------------------------------------| 
| customer_key   | INT          | Surrogate key uniquely identifying each customer record in the dimension table.    | 
| customer_id    | INT          | Unique numerical identifier assigned to each customer.                             |
| customer_number| NVARCHAR(50) | Alphanumeric identifier representing the customer, used for tacking and referncing.|
| first_name     | NVARCHAR(50) | The customer's first name, as recorded in the system.                              | 
| last_name      | NVARCHAR(50) | The customer's last name, as recorded in the sysetm.                               |
| country        | NVARCHAR(50) | The country of residence for the customer (e.g. 'Australia')                       |
| marital_status | NVARCHAR(50) | The marital status of the customer (e.g. 'Married', 'Single')                      |
| gender         | NVARHCAR(50) | The gender of the customer (e.g. 'Male', 'Female', 'Unknown')                      |
| birth_date     | DATE         | The birth date of the customer, formatted as YYYY-MM-DD (e.g. 1971-01-06)          |
| create_date    | DATE         | The date and time when the customer record was created in the system               | 

2. gold.dim_products:
   - ***Purpose:*** provides information about the products and their attributes.
   - ***Columns:***
     
3. gold.fact_sales 
    - ***Purpose:*** provides details of sales transactions with sales amount, price, and quantity.
    - ***Columns:***
