# DynamoDB Single Table Example

The code in this repository uses the Northwind dataset to demonstrate simple relational modeling in a single DynamoDB table. For a full walkthrough of the underlying rationale, see [the accompanying blog post](https://www.trek10.com/blog/dynamodb-single-table-relational-modeling/).

## Using the code
Run `python load.py --setup` with valid AWS credentials in your environment to perform the following actions:
- Create a new DynamoDB table called `northwind` in us-east-2
- Create one GSI on the table
- Load the files in the `csv` folder into the table according to the data access patterns defined in the [blog post](https://trek10.com/blog/dynamodb-single-table-relational-modeling/) that accompanies this project

The table has on-demand capacity enabled, so you shouldn't have to worry about the writes being throttled (or about being charged for capcity when you're not using the table)

If you need to reload the data, run `python load.py`.

Clean up with `python load.py --teardown`.

You can find sample boto3 queries against the data defined in `query.py`.
