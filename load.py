import boto3
import os
import csv
import time
import copy
import argparse

tablename = "northwind"
dynamodb = boto3.resource('dynamodb', region_name="us-east-2")

def setup():
    dynamodb.create_table(TableName=tablename,
        KeySchema=[
            {
                'AttributeName': 'pk',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'sk',
                'KeyType': 'RANGE'
            },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'pk',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'sk',
                'AttributeType': 'S'
            },
            {
            'AttributeName': 'data',
            'AttributeType': 'S'
            }
        ],
        GlobalSecondaryIndexes=[
        {
            'IndexName': 'gsi_1',
            'KeySchema': [
                    {
                        'AttributeName': 'sk',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'data',
                        'KeyType': 'RANGE'
                    },
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL'
                    }
            },
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    print("waiting 60 seconds for table to create...")
    time.sleep(60)


def teardown():
    table = dynamodb.Table(tablename)
    table.delete()

def ddb_batch_write(items):
    dynamodb.batch_write_item(
        RequestItems={tablename : items}
    )
    print(items)

def load_dynamo_data(data):
    while len(data) > 25:
        ddb_batch_write(data[:24])
        data = data[24:]         
    if data:
        ddb_batch_write(data)

def load_csv(filename):
    with open(filename, 'r') as f:
        data = []
        for row in csv.DictReader(f):
            data.append(row)
    return data

def load_csvs():
    categories = load_csv("csv/categories.csv")
    customers = load_csv("csv/customers.csv")
    employees = load_csv("csv/employees.csv")
    orders = load_csv("csv/orders.csv")
    order_details = load_csv("csv/order_details.csv")
    products = load_csv("csv/products.csv")
    shippers = load_csv("csv/shippers.csv")
    suppliers = load_csv("csv/suppliers.csv")

    #clean up data (remove ambiguous IDs)
    for row in categories:
        row["categoryID"] = "categories#"+row["categoryID"]
    for row in suppliers:
        row["supplierID"] = "suppliers#"+row["supplierID"]
    for row in shippers:
        row["shipperID"] = "shippers#"+row["shipperID"]
    for row in products:
        row["productID"] = "products#"+row["productID"]
        row["supplierID"] = "suppliers#"+row["supplierID"]
        row["categoryID"] = "categories#"+row["categoryID"]
    for row in orders:
        row["employeeID"] = "employees#"+row["employeeID"]
        row["shipVia"] = "shippers#"+row["shipVia"]
    for row in order_details:
        row["productID"] = "products#"+row["productID"]
    for row in employees:
        row["employeeID"] = "employees#"+row["employeeID"]
        if row["reportsTo"] and row["reportsTo"] != "NULL":
            row["reportsTo"] = "employees#"+row["reportsTo"]

    return categories, customers, employees, orders, order_details, products, shippers, suppliers

def build_node_list(node_rows, pk, sk, gs1_sk):
    partition = []
    for row in node_rows:
        node_row = copy.deepcopy(row)
        node_row["pk"] = node_row.pop(pk, pk)
        node_row["sk"] = node_row.pop(sk, sk) 
        node_row["data"] = build_composite_sort_key(node_row, gs1_sk) 
        partition.append({'PutRequest': {'Item': node_row}})
    return partition

def build_composite_sort_key(row, keyname):
    elements = keyname.split("#")
    key = [row.pop(element, element) for element in elements]
    return "#".join(key)

def build_adjacency_lists(categories, customers, employees, orders, order_details, products, shippers, suppliers):
    #1. build items for nodes (no edges)
    adjacency_lists = build_node_list(categories, "categoryID", "categoryName", "description")
    adjacency_lists += build_node_list(customers, "customerID", "contactName", "country#region#city#address", )
    adjacency_lists += build_node_list(shippers, "shipperID", "companyName", "phone")
    adjacency_lists += build_node_list(suppliers, "supplierID", "SUPPLIER", "country#region#city#address", )
    adjacency_lists += build_node_list(employees, "employeeID", "reportsTo", "hireDate")
    adjacency_lists += build_node_list(orders, "orderID", "ORDER", "orderDate")
    adjacency_lists += build_node_list(products, "productID", "PRODUCT", "discontinued")
    adjacency_lists += build_node_list(order_details, "orderID", "productID", "unitPrice")

    return adjacency_lists

def handler():
    parser = argparse.ArgumentParser()
    parser.add_argument("--setup", help="create DynamoDB table before loading data", action='store_true')
    parser.add_argument("--teardown", help="delete DynamoDB table", action='store_true')
    args = parser.parse_args()
    if args.teardown:
        teardown()
    else:
        if args.setup:
            setup()
        
        categories, customers, employees, orders, order_details, products, shippers, suppliers = load_csvs()
        table_data = build_adjacency_lists(categories, customers, employees, orders, order_details, products, shippers, suppliers)
        load_dynamo_data(table_data)


if __name__ == "__main__":
    handler()

