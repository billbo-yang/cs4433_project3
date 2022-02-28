import numpy as np
import pandas as pd
import random
import string


# The Customers C dataset should have the following attributes for each customer:
# ID: unique sequential number (integer) from 1 to 50,000 (50,000 lines)
# Name: random sequence of characters of length between 10 and 20 (careful: no commas)
# Age: random number (integer) between 18 to 100
# CountryCode: random number (integer) between 1 and 500
# Salary: random number (float) between 100 and 10,000,

class Customer:
    def __init__(self, id, name, age, country, salary):
        self.id = id
        self.name = name
        self.age = age
        self.country = country
        self.salary = salary

def generate_customers(num):
    # use this string to set the format for the strings, this sets is to be 20 characters long I think
    rows = []
    formatStr = "{0:<10}"
    for i in range(num):
        id = i
        name = ''.join(random.choice(string.ascii_letters) for i in range(15))
        age = 18 + random.randrange(100-18)
        country = random.randrange(500)
        salary = 100 + random.randrange(10000-100)
        customer = Customer(id, name, age, country, salary)
        rows.append(customer.__dict__)
    df = pd.DataFrame(rows)
    return df


# The Purchases P dataset should have the following attributes for each purchase transaction:
# TransID: unique sequential number (integer) from 1 to 5,000,000 (5M purchases)
# CustID: References one of the customer IDs, i.e., on Avg. a customer has 100 transactions.
# TransTotal: Purchase amount as random number (float) between 10 and 2000
# TransNumItems: Number of items as random number (integer) between 1 and 15
# TransDesc: Text of characters of length between 20 and 50 (careful: no commas)

class Purchase:
    def __init__(self, transID, custID, transTotal, transNumItems, transDesc):
        self.transID = transID
        self.custID = custID
        self.transTotal = transTotal
        self.transNumItems = transNumItems
        self.transDesc = transDesc

def generate_purchases(num):
    rows = []
    for i in range(num):
        transID = i
        custID = int(random.uniform(0, 200))
        transTotal = 10+ random.randrange(2000-10)
        transNumItems = random.randrange(15)
        transDesc = ''.join(random.choice(string.ascii_letters) for i in range(25))
        purchase = Purchase(transID, custID, transTotal, transNumItems, transDesc)
        rows.append(purchase.__dict__)
    df = pd.DataFrame(rows)
    return df

num_customers = 500000
customer_df = generate_customers(num_customers)
customer_small_df = generate_customers(int(num_customers/100))

num_transactions = 5000000
transaction_df = generate_purchases(num_transactions)
transaction_small_df = generate_purchases(int(num_transactions/100))

customer_df.to_csv('CUSTOMERS.csv', header=False, index=False)
transaction_df.to_csv('PURCHASES.csv', header=False, index=False)

customer_small_df.to_csv('CUSTOMERS-testing.csv', header=False, index=False)
transaction_small_df.to_csv('PURCHASES-testing.csv', header=False, index=False)
