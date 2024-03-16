# data-vault
-Implementation and verification of a data vault

This project has three files: dataVault.sql, staging.py, information.sql
The first file creates a database and tables in SQL; 
the second one extracts data and loads them into the SQL; 
the third one creates tables for analysis.

-dataVault.sql

--Installation
open the SQL Shell (psql), and type:

cd 'LOCATION OF THE FILE/'

Make sure all "\" are changed into "/". When switch to the right folder, type:

\i dataVault.sql

to create the database and tables for the staging and enterprise layers.

It takes about 245 msec to execute this file.

NOTE: Please don't close this shell, you will use it later for the information.sql.

-staging.py

Before running the script, make sure the dataset is placed in a folder named data 
and the staging.py file is in the code folder next to the data folder.

The structure of the folders should look like this:

/parentFolder
  |- code/
    |- staging.py
    |- dataVault.sql
    |- information.sql
  |- data/
    |- VMData_Blinded
    |- PreAutismData_Blinded

NOTE: Keep the "_Blinded" in the file names!

-- Installation
This script has used the libraries listed below:
pandas, time, numpy, os, psycopg2, re, and hashlib

if any of them is not installed, install it with PyPI:

˙os
  pip install os-sys

˙psycopg2
  pip install psycopg2

˙re
  pip install regex

˙hashlib
  pip install hashlib

˙time
  pip install python-time

˙pandas
  pip install pandas

˙numpy
  pip install numpy

The script is written in python3 and can only be run by python3.

After installing all the libraries, run the file by spyder or cmd.
(Or other ways you want to execute the Python script.)

˙Spyder
  Open Spyder, set the file path to the code folder, and open staging.py 
  and press f5 to run the script

˙cmd
  Open the code folder, click the path bar 
  ( A bar written the path next to the search on the top of a folder)
  type cmd and click enter to execute the cmd 

Now the cmd is supposed located in the code folder and run:

python staging.py

It takes 13'07" to complete.
While running the script, it will print the file it's reading now, 
there are 75 files in VMData_Blinded, and 20 files in PreAutismData_Blinded.

NOTE: The html for in-code documentation is stored at
THE PATH YOU STORE IT\SMD2022_Project\doc\docs\build\html\index.html

-information.sql

--Installation
  Open the shell( psql) you used for executing the dataVault.sql, and type:

  \i information.sql

  It takes 16 secs 514 msec to create views for the business vault and information mart, 
  and tables for information mart.


-A browser-based intuitive GUI for querying 
 This can be accomplished by using Metabase

--Installation
  To run the Metabase, it needs Java JRE.
  By running:

  java -version
  
  To check if Java is installed.
  If it is not installed, download from this link:
  https://adoptium.net/

  Now, download the Metabase file from this link:
  https://www.metabase.com/docs/latest/installation-and-operation/running-the-metabase-jar-file#2-download-metabase

  Create a new directory(Metabase folder) and move the Metabase JAR into it. Open your terminal and location to the Metabase folder

  cd LOCATION OF THE METABASE FOLDER

  run:

  java -jar metabase.jar

  Now you can access Metabase at http://localhost:3000

  You will need to create an account to use the Metabase, and after that connect the smdvault with the parameter below:

  database type: PostgreSQL
  display name: whatever you want
  host: 127.0.0.1
  port: 5432
  user: smd
  database name: smdvault
  password: smd2022
  schema: only
  information <- Type yourself

--Access data
  The seven questions that the information layer should support is created as views, named from question 1 to question 7,
  it can be shown as tables and charts by clicking:

  DATA > Browse data > display name( what you have named it) > question(1-7) > visualization

  You can also create a dashboard to demonstrate those charts or do extra analysis with queries with the fact and dimension tables.


  NOTE: The result figures for question 1 to question 4 in the result folder are done by Metabase.
  

