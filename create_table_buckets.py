#!/usr/bin/env python3
"""
Apache Iceberg Table Operations with AWS Integration
"""

from pyiceberg.catalog import load_catalog
import pyarrow as pa
import pandas as pd
import boto3
import argparse

# Constants
REGION = 'us-east-2'
CATALOG = 's3tablescatalog'
DATABASE = 'myblognamespace'
TABLE_BUCKET = 'pyiceberg-blog-bucket'
TABLE_NAME = 'customers'


def get_aws_account_id():
    """Attempt to get account ID from STS."""
    try:
        sts_client = boto3.client('sts')
        account_id = sts_client.get_caller_identity().get('Account')
        return account_id
    except Exception as e:
        print(f"Error getting account ID: {e}")
        return None


def initialize_catalog(account_id):
    """Initialize catalog using the Glue Iceberg REST endpoint."""
    try:
        rest_catalog = load_catalog(
            CATALOG,
            **{
                "type": "rest",
                "warehouse": f"{account_id}:{CATALOG}/{TABLE_BUCKET}",
                "uri": f"https://glue.{REGION}.amazonaws.com/iceberg",
                "rest.sigv4-enabled": "true",
                "rest.signing-name": "glue",
                "rest.signing-region": REGION,
            },
        )
        print("Catalog loaded successfully!")
        return rest_catalog
    except Exception as e:
        print(f"Error loading catalog: {e}")
        return None


def create_customer_schema():
    """Create and return the PyArrow schema for customer table."""
    return pa.schema(
        [
            pa.field("c_salutation", pa.string()),
            pa.field("c_preferred_cust_flag", pa.string()),
            pa.field("c_first_sales_date_sk", pa.int32()),
            pa.field("c_customer_sk", pa.int32()),
            pa.field("c_first_name", pa.string()),
            pa.field("c_email_address", pa.string()),
        ]
    )


def create_table_if_not_exists(catalog, database, table_name, schema):
    """Check if table exists, create it if it doesn't."""
    try:
        catalog.create_table(identifier=f"{database}.{table_name}", schema=schema)
        print("Table created successfully")
    except Exception as e:
        print(f"Table creation note: {e}")


def load_table(catalog, database, table_name):
    """Load an Iceberg table."""
    try:
        table = catalog.load_table(f"{database}.{table_name}")
        print(f"Table schema: {table.schema()}")
        return table
    except Exception as e:
        print(f"Error loading the table: {e}")
        return None


def overwrite_data(table, data, schema):
    """Overwrite the Iceberg table with new data."""
    try:
        table_data = pa.Table.from_pylist(data, schema=schema)
        table.overwrite(table_data)
        print("Data overwritten successfully!")
    except Exception as e:
        print(f"Error overwriting data: {e}")


def read_table_data(table):
    """Read all data from the Iceberg table and print it."""
    try:
        print("\nReading data from the table...")
        all_data = table.scan().to_pandas()
        print("\nData in the table:")
        print(all_data)
    except Exception as e:
        print(f"Error reading data from the table: {e}")


def main():
    """Main function to orchestrate Iceberg table operations."""
    account_id = get_aws_account_id()
    if not account_id:
        exit()

    catalog = initialize_catalog(account_id)
    if not catalog:
        exit()

    schema = create_customer_schema()

    create_table_if_not_exists(catalog, DATABASE, TABLE_NAME, schema)

    table = load_table(catalog, DATABASE, TABLE_NAME)
    if not table:
        exit()

    # Example data
    initial_data = [
        {
            "c_salutation": "Mr",
            "c_preferred_cust_flag": "Y",
            "c_first_sales_date_sk": 2452737,
            "c_customer_sk": 1235,
            "c_first_name": "Donald",
            "c_email_address": "donald@email.com",
        },
        {
            "c_salutation": "Mrs",
            "c_preferred_cust_flag": "N",
            "c_first_sales_date_sk": 2452738,
            "c_customer_sk": 1236,
            "c_first_name": "Daisy",
            "c_email_address": "daisy@email.com",
        },
    ]

    # Example data to overwrite
    new_data = [
        {
            "c_salutation": "Dr",
            "c_preferred_cust_flag": "Y",
            "c_first_sales_date_sk": 2452739,
            "c_customer_sk": 1237,
            "c_first_name": "Scrooge",
            "c_email_address": "scrooge@email.com",
        }
    ]

    # Overwrite table data
    overwrite_data(table, new_data, schema)

    # Read and print table data after overwrite
    read_table_data(table)


if __name__ == "__main__":
    main()
