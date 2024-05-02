import snowflake.connector as sf

stage_name = "my_stage"
ff_Name = "csv_format_0"
ff_Name_Skip_header = "csvFileFormat"

print("Connecting to the snowflake...")

sf_conn_obj = sf.connect(
    user = 'your_username',
    password = "your_password",
    account = 'your_account_identifier',
    warehouse = 'COMPUTE_WH',
    database = 'MY_DB',
    schema = 'MY_SCHEMA'
)

print("Connected Successfully.")
sf_cursor_obj = sf_conn_obj.cursor()

def check_columns_exist(csv_header_id, table_columns):
    present_columns = [column.split('/')[1] for column in csv_header_id if column.split('/')[0] in table_columns]       # column.split('/')[0].upper() in table_columns or
    all_present = True
    for i in table_columns[:-1]:
        if i not in [column.split('/')[0].upper() for column in csv_header_id] and i not in [column.split('/')[0] for column in csv_header_id]:
            all_present = False
            break
    return all_present, present_columns

try:
    sf_cursor_obj.execute(f"create or replace file format {ff_Name_Skip_header} type=csv skip_header=1")
    result = sf_cursor_obj.fetchone()
    print(result[0])

    sf_cursor_obj.execute(f"create or replace file format {ff_Name} type = csv field_delimiter = ',' parse_header = true")
    result = sf_cursor_obj.fetchone()
    print(result[0])

    # list '@my_stage';
    sf_cursor_obj.execute(f"list @{stage_name}")
    all_files = sf_cursor_obj.fetchall()

    # Show tables
    sf_cursor_obj.execute("show tables")
    all_tables = sf_cursor_obj.fetchall()

    # Extract file names from the paths and convert table names to uppercase for comparison
    file_names = [file[0].split('/')[-1].split('.')[0] for file in all_files]
    table_names = [table[1].upper() for table in all_tables]

    # Iterate through each file name and check if it matches any table name
    for file_name in file_names:
        if file_name.upper() in table_names:
            # Fetch CSV Header
            sf_cursor_obj.execute(f"SELECT * FROM TABLE(INFER_SCHEMA(LOCATION=>'@{stage_name}/{file_name}' , FILE_FORMAT=>'{ff_Name}'));")
            csv_header_id = [f"{i[0]}/{i[5]+1}" for i in sf_cursor_obj.fetchall()]

            # Fetch table columns
            sf_cursor_obj.execute(f"desc table {file_name}")
            desc_tables = sf_cursor_obj.fetchall()
            table_columns = [column[0] for column in desc_tables]

            all_columns_exist, present_columns = check_columns_exist(csv_header_id, table_columns)
            
            if all_columns_exist:
                # Construct SQL statement
                # COPY INTO Nokia_Topology_Transformed 
                # FROM (SELECT $7, $8, $9, $10 FROM @my_stage/Nokia_Topology_Transformed 
                # (FILE_FORMAT => 'csvFileFormat'));

                sql = f"COPY INTO {file_name} FROM (SELECT "
                sql += ", ".join([f"${column}" for column in present_columns]) 
                sql += f", current_timestamp FROM @{stage_name}/{file_name} (FILE_FORMAT => '{ff_Name_Skip_header}'));"
                
                # Create the SQL statement for COPY INTO
                sf_cursor_obj.execute(sql)
                result = sf_cursor_obj.fetchone()
                print(result[0]+": "+file_name)
            else:
                print(f"{file_name} Not all columns in the database table exist in the CSV file.")
        else:
            print(f"File '{file_name}' does not correspond to any table.")

except Exception as e:
    print(e)
finally:
    sf_cursor_obj.close()

sf_conn_obj.close()