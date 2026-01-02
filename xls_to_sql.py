import pandas as pd
import numpy as np

# --- CONFIGURATION ---
input_excel_file = 'latest_food_database_sept_25.xlsx'
output_sql_file = 'food_data_2025_origin.sql'
table_name = 'tb_food_mst_dosm_origin'


# --- 1. DATA CLEANING AND PREPARATION ---

def process_data_to_sql(input_file, output_file, table_name):
    print(f"Reading data from: {input_file}...")
    try:
        # Read the Excel file into a DataFrame
        df = pd.read_excel(input_file)
    except FileNotFoundError:
        print(f"Error: The file '{input_file}' was not found. Please ensure it is in the same directory.")
        return

    # Clean the data as per your original request:
    # 1. Remove semicolons (';') from all columns
    df = df.replace(';', '', regex=True)

    # 2. Convert common missing values/placeholders to a standard NULL indicator
    df = df.replace(['NULL', '0', ''], np.nan)

    # NOTE: The original script set empty strings/NaN to 0.
    # For SQL, setting numeric columns to 0 and string columns to NULL is better practice.

    # The columns derived from your header row
    all_columns = df.columns.tolist()

    # --- 2. SQL TYPE MAPPING (INFERRED) ---
    # Define generic types. Nutrients are DOUBLE for decimals. Names are VARCHAR.

    # Default type is VARCHAR for safety
    column_types = {col: 'VARCHAR(255)' for col in all_columns}

    # Identify numeric columns (most nutrient and serving data)
    numeric_cols = all_columns[11:130]  # Start at 'energy', end before 'threonine'

    # Identify amino acids/other numericals
    numeric_cols += all_columns[130:144]  # 'threonine' through 'serine'

    # Identify serving/weight columns
    serving_cols = all_columns[147::2]  # Serv amount and weight columns (every other one after serv_amount_1)

    for col in numeric_cols:
        column_types[col] = 'DOUBLE'

    # Specific overrides:
    column_types['food_id'] = 'VARCHAR(50) PRIMARY KEY'  # Set a primary key
    column_types['ndb_no'] = 'INT'
    column_types['last_modified'] = 'DATETIME'

    # --- 3. SQL GENERATION ---

    with open(output_file, 'w', encoding='utf-8') as f:
        print(f"Generating SQL file: {output_file}...")

        # --- A. DROP & CREATE TABLE STATEMENTS ---
        f.write(f"DROP TABLE IF EXISTS `{table_name}`;\n\n")

        create_table_sql = f"CREATE TABLE `{table_name}` (\n"

        # Track column definitions
        col_defs = []
        for col_name in all_columns:
            sql_type = column_types.get(col_name, 'VARCHAR(255)')

            # Simple NULL definition for all non-primary key fields
            null_def = 'NOT NULL' if 'PRIMARY KEY' in sql_type else 'NULL'

            # Remove PRIMARY KEY from the type if present for clean definition
            type_only = sql_type.replace(' PRIMARY KEY', '')

            col_defs.append(f"  `{col_name}` {type_only} {null_def}")

        create_table_sql += ",\n".join(col_defs)
        create_table_sql += "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n\n"

        f.write(create_table_sql)

        # --- B. INSERT STATEMENTS ---

        # Convert all numeric columns to float, coercion errors will result in NaN (which we treat as NULL)
        for col, dtype in column_types.items():
            if dtype == 'DOUBLE':
                # Attempt to convert to numeric, errors='coerce' turns bad data into NaN
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Replace remaining NaN (missing/cleaned data) with SQL NULL keyword
        df_sql = df.astype(object).where(pd.notna(df), None)

        # Start insert loop
        f.write(f"START TRANSACTION;\n")

        insert_template = f"INSERT INTO `{table_name}` (`{'`, `'.join(all_columns)}`) VALUES ("

        for index, row in df_sql.iterrows():
            values = []
            for col_name in all_columns:
                value = row[col_name]

                if value is None:
                    # Use standard SQL NULL keyword
                    values.append('NULL')
                elif column_types.get(col_name) in ['INT', 'DOUBLE']:
                    # Numeric values are written directly
                    values.append(str(value))
                else:
                    # String/Date values are wrapped in quotes and escaped
                    # Escape single quotes and backslashes for SQL safety
                    safe_value = str(value).replace("'", "''").replace("\\", "\\\\")
                    values.append(f"'{safe_value}'")

            f.write(insert_template + f"{', '.join(values)});\n")

        f.write(f"COMMIT;\n")
        print(f"SQL file '{output_file}' generated successfully with {len(df)} INSERT statements.")


# Execute the function
process_data_to_sql(input_excel_file, output_sql_file, table_name)
