from abc import update_abstractmethods
from snowflake.snowpark import Session
import snowflake.snowpark.functions as F

def table_exists(session, schema='', name=''):
    exists = session.sql("SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}') AS TABLE_EXISTS".format(schema, name)).collect()[0]['TABLE_EXISTS']
    return exists
 
def create_orders_table(session):
    _ = session.sql("CREATE TABLE HARMONIZED.ORDERS LIKE HARMONIZED.POS_FLATTENED_V").collect()
    _ = session.sql("ALTER TABLE HARMONIZED.ORDERS ADD COLUMN META_UPDATED_AT TIMESTAMP").collect()
 
def create_orders_stream(session):
    _ = session.sql("CREATE STREAM HARMONIZED.ORDERS_STREAM ON TABLE HARMONIZED.ORDERS").collect()
 
def merge_order_updates(session):
    _ = session.sql('ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE').collect()

    existing_data = session.table('HARMONIZED."ORDERS"')
    source_data = session.table('HARMONIZED."POS_FLATTENED_V_STREAM"')

    merge_sql = f"""
        MERGE INTO HARMONIZED."ORDERS" AS target
        USING HARMONIZED."POS_FLATTENED_V_STREAM" AS source
        ON target."ORDER_DETAIL_ID" = source."ORDER_DETAIL_ID"
        WHEN MATCHED THEN
            UPDATE SET target."COL1" = source."COL1", target."COL2" = source."COL2", ...
        WHEN NOT MATCHED THEN
            INSERT ("COL1", "COL2", ...)
            VALUES (source."COL1", source."COL2", ...)
    """
    _ = session.sql(merge_sql).collect()

    _ = session.sql('ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XSMALL').collect()
 
def main(session: Session) -> str:
    # Create the ORDERS table and ORDERS_STREAM stream if they don't exist
    if not table_exists(session, schema='HARMONIZED', name='ORDERS'):
        create_orders_table(session)
        create_orders_stream(session)


 
    # Process data incrementally
    merge_order_updates(session)
    return f"Successfully processed ORDERS"



 

# For local debugging
if __name__ == '__main__':
    import os, sys
    current_dir = os.getcwd()
    parent_parent_dir = os.path.dirname(os.path.dirname(current_dir))
    sys.path.append(parent_parent_dir)
 
    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()
 
    if len(sys.argv) > 1:
        print(main(session, *sys.argv[1:]))
    else:
        print(main(session))
 
    session.close()