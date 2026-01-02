import pandas as pd
import numpy as np

# --- CONFIGURATION ---
# IMPORTANT: Ensure this file is present and correctly named
input_excel_file = 'latest_food_database_sept_25.xlsx'
output_sql_file = 'food_data_2025_fixed.sql'  # IMPORTANT: New file name
table_name = 'tb_food_mst_dosm_origin'


# --- 1. DATA CLEANING AND PREPARATION ---

def process_data_to_sql(input_file, output_file, table_name):
    print(f"Reading data from: {input_file} and forcing string types for key columns...")

    # CRITICAL FIX: Explicitly define columns that contain mixed data (like 'food_id')
    # as strings ('object') to prevent Pandas from inferring them as numeric.
    dtype_mapping = {
        'food_id': str,
        'ndb_no': str,  # Treat NDB number as string just in case
        'item code': str,
    }

    try:
        # Read the Excel file, forcing the data types for critical columns
        df = pd.read_excel(input_file, dtype=dtype_mapping)
    except FileNotFoundError:
        print(f"Error: The file '{input_file}' was not found. Please ensure it is in the same directory.")
        return
    except ImportError as e:
        print(f"Error: {e}")
        print("Please run 'pip install openpyxl' to install the necessary library.")
        return

    # 1. Standard cleaning: Remove semicolons and ensure all column names are cleaned
    df.columns = df.columns.str.replace(';', '').str.strip()
    df = df.replace(';', '', regex=True)

    # 2. Convert common missing values/placeholders to a standard NULL indicator (np.nan)
    df = df.replace(['NULL', '0', ''], np.nan)

    all_columns = df.columns.tolist()

    # --- 2. SQL TYPE MAPPING (INFERRED) ---
    column_types = {col: 'VARCHAR(255)' for col in all_columns}

    # Identify numeric columns (most nutrient and serving data)
    try:
        start_index = all_columns.index('energy')
        end_index = all_columns.index('serv_weight_7') + 1

        numeric_cols = all_columns[start_index:end_index]
        for col in numeric_cols:
            column_types[col] = 'DOUBLE'
    except ValueError:
        print("Warning: Could not find nutrient start/end columns. Defaulting all to VARCHAR.")

    # Specific overrides:
    column_types['food_id'] = 'VARCHAR(50) PRIMARY KEY'
    column_types['ndb_no'] = 'INT'
    column_types['last_modified'] = 'DATETIME'

    # Convert all identified numeric columns to float, coercing errors to NaN
    for col, dtype in column_types.items():
        if dtype in ['INT', 'DOUBLE']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # --- 3. SQL GENERATION ---

    with open(output_file, 'w', encoding='utf-8') as f:
        print(f"Generating SQL file: {output_file}...")

        # --- A. DROP & CREATE TABLE STATEMENTS ---
        f.write(f"DROP TABLE IF EXISTS `{table_name}`;\n\n")

        create_table_sql = f"CREATE TABLE `{table_name}` (\n"

        col_defs = []
        for col_name in all_columns:
            sql_type = column_types.get(col_name, 'VARCHAR(255)')
            null_def = 'NOT NULL' if 'PRIMARY KEY' in sql_type else 'NULL'
            type_only = sql_type.replace(' PRIMARY KEY', '')
            col_defs.append(f"  `{col_name}` {type_only} {null_def}")

        create_table_sql += ",\n".join(col_defs)
        create_table_sql += "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n\n"

        f.write(create_table_sql)

        # --- B. INSERT STATEMENTS ---

        df_sql = df.astype(object).where(pd.notna(df), None)

        f.write(f"START TRANSACTION;\n")

        insert_template = f"INSERT INTO `{table_name}` (`{'`, `'.join(all_columns)}`) VALUES ("

        for index, row in df_sql.iterrows():
            values = []
            for col_name in all_columns:
                value = row[col_name]

                if value is None:
                    values.append('NULL')
                elif column_types.get(col_name) in ['INT', 'DOUBLE']:
                    # Numeric values (safe to write without quotes)
                    # Use strip() to clean any accidental whitespace from the string before conversion
                    values.append(str(value).strip())
                else:
                    # String, DATETIME, and primary key values (MUST be quoted)
                    safe_value = str(value).replace("'", "''").replace("\\", "\\\\")
                    values.append(f"'{safe_value}'")

            f.write(insert_template + f"{', '.join(values)});\n")

        f.write(f"COMMIT;\n")
        print(f"SQL file '{output_file}' generated successfully with {len(df)} INSERT statements.")


# Execute the function
process_data_to_sql(input_excel_file, output_sql_file, table_name)
