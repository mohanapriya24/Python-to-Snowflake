# Python-to-Snowflake
This program Connect python and snowflake using json and insertvalues to the existing table and do select statement in python.
add python interpreters snowflake.connector and pandas.
Enter your snowflake accounts username, password, account, warehouse, database, schema, role in the json file.
This will connect the snowflake account to python using snowflake.connector.
Enter the path correctly where the json and csv file is placed.
The master csv file contains the link to the location of the csv files that are to be inserted and next to that will be the table name in your snowflake account to which the values are to be inserted.
Make sure that the table column and the csv file contains same number of attributes and datatype of the attributes.
If there is any error it will be inserted in the errorRecord.txt file.
