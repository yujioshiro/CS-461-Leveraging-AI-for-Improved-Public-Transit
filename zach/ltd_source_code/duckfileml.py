#-----------
# Author: Zach Benedetti
# Date: 3/9/25
# Purpose of File: Clear/fill null data in batches
#-----------
import pandas as pd
import duckdb
from sklearn.impute import SimpleImputer
import gc

# Connect to the DuckDB database
con = duckdb.connect("ltdinit.db")
query = "SELECT * FROM my_table LIMIT 1000"  # Limiting to first 1000 rows for testing
df = con.execute(query).fetchdf()

# Check percentage of nulls for each column and only keep those with > 0% null
null_percent = df.isnull().mean() * 100
columns_to_impute = null_percent[null_percent > 0].index.tolist()

# Drop columns that have all null values
df = df.dropna(axis=1, how='all')

# Update the list of columns to impute to reflect the ones that still exist after dropping
columns_to_impute = [col for col in columns_to_impute if col in df.columns]

# Separate numeric and categorical columns for differed imputation strategies
numeric_columns = df[columns_to_impute].select_dtypes(include=['float64', 'int64']).columns.tolist()
categorical_columns = df[columns_to_impute].select_dtypes(include=['object']).columns.tolist()

# Set up the imputer for numeric columns (MEAN imputation)
numeric_imputer = SimpleImputer(strategy='mean')

# Set up the imputer for categorical columns (MODE imputation)
categorical_imputer = SimpleImputer(strategy='most_frequent')

# Batch Processing Method
def process_in_batches(batch_size=1000):
    total_rows = df.shape[0]
    num_batches = (total_rows // batch_size) + 1
    
    # Processing each batch
    for batch_num in range(num_batches):
        start_row = batch_num * batch_size
        end_row = start_row + batch_size
        df_batch = df.iloc[start_row:end_row]
        
        # Checking for valid imputation rows
        if not df_batch.empty:
            # Impute missing values for numeric columns
            if numeric_columns:
                df_batch[numeric_columns] = numeric_imputer.fit_transform(df_batch[numeric_columns])
            
            # Impute missing values for categorical columns
            if categorical_columns:
                df_batch[categorical_columns] = categorical_imputer.fit_transform(df_batch[categorical_columns])
            
            # Connect to a new DuckDB database to insert data into aggregated_data table
            new_con = duckdb.connect("new_database.db")
            new_con.execute("DROP TABLE IF EXISTS aggregated_data")

            # Insert batch into table
            df_batch.to_sql('aggregated_data', new_con, if_exists='replace', index=False)

            # Close the new connection
            new_con.close()

            # Clear memory
            del df_batch
            gc.collect()

    print("Batch processing completed.")

# Batch processing function
process_in_batches(batch_size=1000)

# Close original connection
con.close()


