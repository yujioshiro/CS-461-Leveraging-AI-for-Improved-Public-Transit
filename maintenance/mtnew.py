import duckdb

# Connect to the Duckdb Database (name is changable)
con = duckdb.connect("mtdata.duckdb")

# Loads parquet file into a table called "my_table", you can change the name if you guys want
con.execute("""
    CREATE TABLE IF NOT EXISTS my_table AS 
    SELECT * FROM 'ltd-EAM_TSK_MAIN.parquet'
""")

# This is just an error check
print(con.execute("SELECT * FROM my_table LIMIT 5").df())

con.close()
