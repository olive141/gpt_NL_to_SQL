import pyodbc
import pandas as pd
import json

with open(r'config.json') as config_file:
  config_details = json.load(config_file)

server = config_details['SERVER']   
database = config_details['DATABASE']  
username = config_details['USER']  
password = config_details['PASSWORD']  

cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

# Connect to SQL Server
print("Connecting to SQL Server")
cursor = cnxn.cursor()

print("Creating Tables")
# CREATE TABLES
cursor.execute("""
CREATE TABLE Customers (
  cust_id INTEGER,
  cust_name VARCHAR(1000),
  cust_email VARCHAR(1000),
  cust_phone VARCHAR(1000),
  cust_address VARCHAR(1000),
  PRIMARY KEY (cust_id)
);

CREATE TABLE Products (
  prod_id INTEGER,
  prod_name VARCHAR(1000),
  price FLOAT,
  category VARCHAR(1000),
  PRIMARY KEY (prod_id)
);

CREATE TABLE Merchants (
  merchant_id INTEGER,
  merchant_name VARCHAR(1000),
  merchant_region VARCHAR(1000),
  merchant_address VARCHAR(1000),
  PRIMARY KEY (merchant_id)
);

CREATE TABLE Stock (
  prod_id INTEGER,
  merchant_id INTEGER,
  stock INTEGER,
  PRIMARY KEY (prod_id, merchant_id),
  FOREIGN KEY (merchant_id) REFERENCES Merchants(merchant_id),
  FOREIGN KEY (prod_id) REFERENCES Products(prod_id)
);

CREATE TABLE Sales (
    sale_id INTEGER,
    cust_id INTEGER,
    merchant_id INTEGER,
    date DATETIME,
    total_price FLOAT,
    PRIMARY KEY (sale_id),
    FOREIGN KEY (cust_id) REFERENCES Customers(cust_id),
    FOREIGN KEY (merchant_id) REFERENCES Merchants(merchant_id)
);

CREATE TABLE Sales_Detail (
  sales_id INTEGER,
  prod_id INTEGER,
  quantity INTEGER,
  PRIMARY KEY (sales_id, prod_id),
  FOREIGN KEY (sales_id) REFERENCES Sales(sale_id),
  FOREIGN KEY (prod_id) REFERENCES Products(prod_id)
);
""") 

# Populate Data
print("Populating Data")

def insert_data(cursor, inject_query, data_path):
    data = pd.read_csv (data_path)   
    df = pd.DataFrame(data)
    data = df.values.tolist()

    cursor.fast_executemany = True
    cursor.executemany(inject_query, data)

insert_data(cursor, "INSERT INTO Customers (cust_id,cust_name,cust_email,cust_phone,cust_address) VALUES (?,?,?,?,?)", 'sql_data/customers.csv')
insert_data(cursor, "INSERT INTO Products (prod_id, prod_name, price, category) VALUES (?,?,?,?)", 'sql_data/products.csv')
insert_data(cursor, "INSERT INTO Merchants (merchant_id,merchant_name,merchant_region,merchant_address) VALUES (?,?,?,?)", 'sql_data/merchants.csv')
insert_data(cursor, "INSERT INTO Stock (prod_id,merchant_id,stock) VALUES (?,?,?)", 'sql_data/stock.csv')
insert_data(cursor, "INSERT INTO Sales (sale_id,cust_id,merchant_id,date,total_price) VALUES (?,?,?,?,?)", 'sql_data/sales.csv')
insert_data(cursor, "INSERT INTO Sales_Detail (sales_id,prod_id,quantity) VALUES (?,?,?)", 'sql_data/sales_detail.csv')

cnxn.commit()