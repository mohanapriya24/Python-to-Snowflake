# Python-to-Snowflake
* This program is to create connection between python and snowflake using json.
* The process is possible by adding interpreters such as **snowflake.connector** and **pandas** in the python.
* Open the json file and enter your snowflake accounts username, password, account, warehouse, database, schema, role in the specified area.
* The path for json and csv file should be mentioned properly in the python code.
* This will help in connecting the python with your snowflake account using snowflake.connector interpreter.
* After connection is established we can insert new data to the existing table and view the data using select statement.
* The master csv file contains the link to the location of the csv files which are to be inserted followed by the table name wich is available in your snowflake account where the data should be inserted.
* Verify if the table in snowflake and data in csv file contains same number of attributes and datatype to avoid errors.
* If any error occures it will be maintained in the errorRecord.txt file for references.
