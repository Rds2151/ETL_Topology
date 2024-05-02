import os
import sys
import snowflake.connector as sf

stage_name = "my_stage"
ff_Name = "csvFileFormat"
# Get the filename from command-line arguments
if len(sys.argv) < 2:
    print("Usage: python script.py <filename>")
    sys.exit(1)

filename = sys.argv[1]

# Check if the file exists and is a valid file
if os.path.isfile(filename):
    print(f"The file '{filename}' exists and is a valid file.")
else:
    print(f"The file '{filename}' does not exist or is not a valid file.")
    sys.exit(1)


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

try:
    sf_cursor_obj.execute(f"create stage if not exists my_stage")
    result = sf_cursor_obj.fetchall()
    print(result)

    # list '@my_stage';
    sf_cursor_obj.execute(f"put file://{filename} @my_stage OVERWRITE=true")
    result = sf_cursor_obj.fetchall()
    print(result)
    
except Exception as e:
    print(e)
finally:
    sf_cursor_obj.close()

sf_conn_obj.close()