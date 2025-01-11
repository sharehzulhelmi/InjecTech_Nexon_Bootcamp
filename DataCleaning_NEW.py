import pyodbc
import pandas as pd
import numpy as np
from datetime import datetime
 
# Define the SQL Server connection details
server = # AEM server
database =# Target database
username = # AEM username 
password = # AEM password 
 
# Step 1: Establish a connection to the database
# Replace with your actual database connection string
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()
table_name = 'dbo.ITN_CHEMICAL_INJECTION'

 
# Load the Excel file
file_path = 'C:/Users/AEM-Raihan/Desktop/Dummy Infinidata/Dummy.xlsx'  # Replace with your file path
output_file = 'C:/Users/AEM-Raihan/Desktop/Dummy Infinidata/updated_excel_file.xlsx'  # Name for the output file
log_file = 'C:/Users/AEM-Raihan/Desktop/Dummy Infinidata/validation_log.txt'  # Name for the log file'
 
# Load the data into a DataFrame
df = pd.read_excel(file_path)
 
# Get Platform Id
queryString = "SELECT PLATFORM_CODE_ID, PLATFORM_CODE FROM dbo.ITN_PLATFORM"
cursor.execute(queryString)
result = cursor.fetchall()
for i, j in result:
    df.replace([j], i, inplace=True)
 
# Get Chemical Code Id
queryString = "SELECT CHEMICAL_CODE_ID, CHEMICAL_CODE FROM dbo.ITN_CHEMICAL"
cursor.execute(queryString)
result = cursor.fetchall()
for i, j in result:
    df.replace([j], i, inplace=True)

# Step 1: Replace "null" or "Null" with blanks
df.replace(['null', 'Null'], '', inplace=True)

# Replace NaN values with 0
df.fillna(0, inplace=True)

# Renaming multiple columns
# Renaming multiple columns
df.rename(columns={'ACTUAL_INJECTION_RATE  (L/day)': 'ACTUAL_INJECTION_RATE', 'RECOMMENDED_INJECTION_RATE (L/day)': 'RECOMMENDED_INJECTION_RATE',
                    'CHEMICAL_DOSAGE_COMPLIANCE (%)': 'CHEMICAL_DOSAGE_COMPLIANCE', 'CHEMICAL_CODE': 'CHEMICAL_CODE_ID', 
                    'PLATFORM_CODE': 'PLATFORM_CODE_ID'}, inplace=True)
#df.replace(['ACTUAL_INJECTION_RATE  (L/day)'], 'ACTUAL_INJECTION_RATE', inplace=True)
#df.replace(['RECOMMENDED_INJECTION_RATE (L/day)'], 'RECOMMENDED_INJECTION_RATE', inplace=True)
#df.replace(['CHEMICAL_DOSAGE_COMPLIANCE (%)'], 'CHEMICAL_DOSAGE_COMPLIANCE', inplace=True)
print(df.head())

# Step 2: Add DELETE_FLAG for rows with negative values
columns_to_check_negative = ["ACTUAL_INJECTION_RATE", "ACTUAL_DOSAGE"]

# Create DELETE_FLAG column, default is 0
df["DELETED_FLAG"] = 0

# Set DELETE_FLAG to 1 where any specified column has negative values
df.loc[(df[columns_to_check_negative] < 0).any(axis=1), "DELETED_FLAG"] = 1

# Step 3: Calculate "CHEMICAL_DOSAGE_COMPLIANCE (%)"
df["CHEMICAL_DOSAGE_COMPLIANCE"] = np.where(
    (df["ACTUAL_DOSAGE"] == 0) | (df["RECOMMENDED_DOSAGE"] == 0),
    0,  # If condition is true
    (df["ACTUAL_DOSAGE"] / df["RECOMMENDED_DOSAGE"]) * 100  # Else calculate percentage
)

# Step 4: Set "GAS_BOE" to 0 where "GAS_RATE" is 0
df.loc[df["GAS_RATE"] == 0, "GAS_BOE"] = 0

# Step 5: Validate numeric columns and log non-numeric values
columns_to_validate = [
    "GROSS_RATE", 
    "WATER_RATE", 
    "GAS_RATE", 
    "GAS_BOE", 
    "ACTUAL_INJECTION_RATE", 
    "ACTUAL_DOSAGE", 
    "RECOMMENDED_DOSAGE", 
    "RECOMMENDED_INJECTION_RATE", 
    "CHEMICAL_DOSAGE_COMPLIANCE"
]

# Open the log file
with open(log_file, 'w') as log:
    log.write("Validation Log\n")
    log.write("=" * 50 + "\n")
    
    for col in columns_to_validate:
        for index, value in df[col].items():
            if isinstance(value, str) and not value.replace('.', '', 1).isdigit():
                log.write(f"Non-numeric value found: {value} at Row {index + 2}, Column '{col}'\n")
                df.at[index, col] = np.nan

# Step 6: Remove specified columns
columns_to_remove = [
    "REGION_CODE", 
    "FIELD_CODE", 
    "FIELD_NAME", 
    "PLATFORM_NAME", 
    "CHEMICAL_CATEGORY", 
    "CHEMICAL_TYPE"
]

for col in columns_to_remove:
    if col in df.columns:
        df.drop(columns=[col], inplace=True)

# Add a new column with the current timestamp
df['DATA_UPDATED'] = datetime.now()

# Specify the desired column order
desired_columns = [
    'CHEMICAL_CODE_ID',
    'PLATFORM_CODE_ID',
    'CHEMICAL_INJECTION_POINT',
    'CHEMICAL_INJECTION_POINT_TYPE',
    'START_DATETIME',
    'GROSS_RATE',
    'WATER_RATE',
    'GAS_RATE',
    'GAS_BOE',
    'ACTUAL_INJECTION_RATE',
    'ACTUAL_DOSAGE',
    'ACTUAL_DOSAGE_UOM',
    'RECOMMENDED_DOSAGE',
    'RECOMMENDED_DOSAGE_UOM',
    'RECOMMENDED_INJECTION_RATE',
    'CHEMICAL_DOSAGE_COMPLIANCE',
    'REMARKS',
    'DATA_SOURCE',
    'DATA_UPDATED',
    'DELETED_FLAG'
]

# Reorder columns in the DataFrame
df = df[desired_columns]

# Save the final DataFrame back to an Excel file
df.to_excel(output_file, index=False)

print(f"Final processed file saved as {output_file}")
print(f"Validation log saved as {log_file}")

# Load Data
try:
    # Insert the data row by row (or in batches if needed)
    for index, row in df.iterrows():
        columns = ", ".join(f"[{col}]" for col in df.columns)
        placeholders = ", ".join("?" for _ in df.columns)
        insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        print(row)
        cursor.execute(insert_sql, *row)
       
        # Commit after every 1000 rows to improve performance
        if index % 1000 == 0:
            conn.commit()
    # Commit the transaction after all rows are inserted
    conn.commit()
    print("Data loaded successfully!")
except Exception as e:
    # If there's an error, rollback the transaction
    conn.rollback()
    print("Error loading data:", e)
finally:
    # Close the connection and cursor
    cursor.close()
    conn.close()