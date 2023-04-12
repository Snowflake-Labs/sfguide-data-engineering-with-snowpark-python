### Create Snowflake Credentials File
In the Codespace terminal execute the following commands:
```
mkdir ~/.snowsql
vi ~/.snowsql/config
```

Add your account details to the create credentials file for snowsql, which are the exact same values used for the Github secrets. Copy the credentials from this file into vi. To save and exit, execute shift + ZZ in the terminal:
Note: we aren’t actually installing or using snowsql, just creating the credentials in the location that the snowpark_utils python file expects them to be, since we are just deploying code to Snowflake and not staging local data.
If you have successfully completed all the steps, congratulations you are ready for the Hands on Lab! If you completed these prerequisites prior to attending the Hands on Lab, you can stop the Codespace in Github where you launched it from, or it will automatically stop after 30 mintues

#### Create Snowsql Credentials File
```
[connections.dev]
accountname = myaccount
username = myusername
password = "mypassword"
rolename = HOL_ROLE
warehousename = HOL_WH
dbname = HOL_DB
```

Note: Password will need to be escaped with double quotes if it contains special characters for example "sdfT*#092$"
