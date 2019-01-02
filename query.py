import boto3
from boto3.dynamodb.conditions import Key

tablename = "northwind"
dynamodb = boto3.resource('dynamodb', region_name="us-east-2")
table = dynamodb.Table(tablename)

# a. Get employee by employee ID
response = table.query(KeyConditionExpression=Key('pk').eq('employees#2'))
print(response['Items'])

# b. Get direct reports for an employee
response = table.query(IndexName='gsi_1',KeyConditionExpression=Key('sk').eq('employees#2'))
print(response['Items'])

# c. Get discontinued products
response = table.query(IndexName='gsi_1',KeyConditionExpression=Key('sk').eq('PRODUCT') & Key('data').eq('1'))
print(response['Items'])

# d. List all orders of a given product
response = table.query(IndexName='gsi_1',KeyConditionExpression=Key('sk').eq('products#1'))
print(response['Items'])

# e. Get the most recent 25 orders
response = table.query(IndexName='gsi_1',KeyConditionExpression=Key('sk').eq('ORDER'), Limit=25)
print(response['Items'])

# f. Get shippers by name
response = table.query(IndexName='gsi_1',KeyConditionExpression=Key('sk').eq('United Package'))
print(response['Items'])

# g. Get customers by contact name
response = table.query(IndexName='gsi_1',KeyConditionExpression=Key('sk').eq('Maria Anders'))
print(response['Items'])

# h. List all products included in an order
response = table.query(KeyConditionExpression=Key('pk').eq('10260') & Key('sk').begins_with('product'))
print(response['Items'])

# i. Get suppliers by country and region
response = table.query(IndexName='gsi_1',KeyConditionExpression=Key('sk').eq('SUPPLIER') & Key('data').begins_with('Germany#NULL'))
print(response['Items'])