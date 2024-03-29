
# Implementation and Verification of a Data Vault

This project involves the implementation and verification of a data vault system. It comprises three main components: `dataVault.sql`, `staging.py`, and `information.sql`. Each file serves a specific purpose in the overall process.

## dataVault.sql

This file sets up the database and necessary tables in SQL. To install and execute it:

1. Open the SQL Shell (psql).
2. Navigate to the directory where `dataVault.sql` is located.
3. Execute the following command: `\i dataVault.sql`

This script creates the required tables for the staging and enterprise layers. It typically takes about 245 milliseconds to execute. Ensure that the SQL Shell remains open for later use with `information.sql`.

## staging.py

This Python script is responsible for extracting data and loading it into the SQL database. Before running the script, ensure the following folder structure:
```
/parentFolder
|- code/
   |- staging.py
|- dataVault.sql
|- information.sql
|- data/
   |- VMData_Blinded
   |- PreAutismData_Blinded
```
### Installation of Dependencies

Ensure the following libraries are installed using PyPI:

- `pandas`
- `time`
- `numpy`
- `os`
- `psycopg2`
- `re`
- `hashlib`

You can install these libraries using pip, for example: `pip install pandas`.

### Running the Script

You can run the script using Spyder or the command line:

- **Spyder**: Open Spyder, set the file path to the code folder, open `staging.py`, and press `F5` to run the script.
  
- **Command Line (cmd)**: Navigate to the code folder in the command prompt and run: `python staging.py`

During execution, the script will display the file it's currently reading. There are 75 files in `VMData_Blinded` and 20 files in `PreAutismData_Blinded`.

## information.sql

This SQL file creates views for the business vault and information mart, along with tables for the information mart. To install:

1. Open the SQL Shell (psql).
2. Execute the following command: `\i information.sql`

This script typically takes 16 seconds and 514 milliseconds to execute. It creates necessary views and tables for the information layer.

## Browser-based GUI for Querying (Metabase)

This project utilizes Metabase for querying data through an intuitive GUI.

### Installation

Before running Metabase, ensure Java JRE is installed. You can check by running `java -version`. If not installed, download Java from [this link](https://adoptium.net/).

To run Metabase:

1. Download the Metabase JAR file from [here](https://www.metabase.com/docs/latest/installation-and-operation/running-the-metabase-jar-file#2-download-metabase).
2. Create a new directory (Metabase folder) and move the Metabase JAR into it.
3. Open your terminal and navigate to the Metabase folder.
4. Run: `java -jar metabase.jar`
5. Access Metabase at http://localhost:3000.

### Accessing Data

1. Create an account on Metabase.
2. Connect the data vault with the following parameters:
   - Database type: PostgreSQL
   - Display name: Whatever you prefer
   - Host: 127.0.0.1
   - Port: 5432
   - User: smd
   - Database name: smdvault
   - Password: smd2022
   - Schema: information
