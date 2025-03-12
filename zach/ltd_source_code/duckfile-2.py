#-----------
# Author: Zach Benedetti
# Date: 3/9/25
# Purpose of File: Compute a SQL Query for Column Names in data from Aggregated Data
#-----------
import duckdb

# Connect to the DuckDB database
con = duckdb.connect('new_database.db')

# SQL Query
query = """
    PRAGMA table_info(aggregated_data)
"""
column_info = con.execute(query).fetchall()

# Extract column names
column_names = [column[1] for column in column_info]

# Print the column names
print("Column names in 'aggregated_data':")
for name in column_names:
    print(name)

# Close the connection
con.close()
