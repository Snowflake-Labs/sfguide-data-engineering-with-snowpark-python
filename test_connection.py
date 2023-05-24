import configparser
import os
import sys
from snowflake.snowpark import Session
from utils.snowpark_utils import get_snowsql_config
from snowflake.connector.errors import DatabaseError, OperationalError

if __name__ == "__main__":
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)
    snowpark_config = get_snowsql_config()
    snowpark_config['ROLE'] = 'ACCOUNTADMIN'

    try:
        session = Session.builder.configs(snowpark_config).create()
        account = session.get_current_account()
        print("Success you're connected to Snowflake account: " + account)
        print("Enjoy the Hands-on Lab!")
    except OperationalError as oe:
        print(oe.msg)
    except DatabaseError as de:
        print(de.msg)




