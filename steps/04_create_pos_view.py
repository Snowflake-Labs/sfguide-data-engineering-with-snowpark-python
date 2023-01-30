#------------------------------------------------------------------------------
# Hands-On Lab: Data Engineering with Snowpark
# Script:       04_create_order_view.py
# Author:       Jeremiah Hansen, Caleb Baechtold
# Last Updated: 1/9/2023
#------------------------------------------------------------------------------

# SNOWFLAKE ADVANTAGE: Snowpark DataFrame API
# SNOWFLAKE ADVANTAGE: Streams for incremental processing (CDC)
# SNOWFLAKE ADVANTAGE: Streams on views


from snowflake.snowpark import Session
#import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F


def create_pos_view(session):
    session.use_schema('HARMONIZED')
    order_detail = session.table("RAW_POS.ORDER_DETAIL").select(F.col("ORDER_DETAIL_ID"), \
                                                                F.col("LINE_NUMBER"), \
                                                                F.col("MENU_ITEM_ID"), \
                                                                F.col("QUANTITY"), \
                                                                F.col("UNIT_PRICE"), \
                                                                F.col("PRICE"), \
                                                                F.col("ORDER_ID"))
    order_header = session.table("RAW_POS.ORDER_HEADER").select(F.col("ORDER_ID"), \
                                                                F.col("TRUCK_ID"), \
                                                                F.col("ORDER_TS"), \
                                                                F.to_date(F.col("ORDER_TS")).alias("ORDER_TS_DATE"), \
                                                                F.col("ORDER_AMOUNT"), \
                                                                F.col("ORDER_TAX_AMOUNT"), \
                                                                F.col("ORDER_DISCOUNT_AMOUNT"), \
                                                                F.col("LOCATION_ID"), \
                                                                F.col("ORDER_TOTAL"))
    truck = session.table("RAW_POS.TRUCK").select(F.col("TRUCK_ID"), \
                                                F.col("PRIMARY_CITY"), \
                                                F.col("REGION"), \
                                                F.col("COUNTRY"), \
                                                F.col("FRANCHISE_FLAG"), \
                                                F.col("FRANCHISE_ID"))
    menu = session.table("RAW_POS.MENU").select(F.col("MENU_ITEM_ID"), \
                                                F.col("TRUCK_BRAND_NAME"), \
                                                F.col("MENU_TYPE"), \
                                                F.col("MENU_ITEM_NAME"))
    franchise = session.table("RAW_POS.FRANCHISE").select(F.col("FRANCHISE_ID"), \
                                                        F.col("FIRST_NAME").alias("FRANCHISEE_FIRST_NAME"), \
                                                        F.col("LAST_NAME").alias("FRANCHISEE_LAST_NAME"))
    location = session.table("RAW_POS.LOCATION").select(F.col("LOCATION_ID"))

    
    '''
    We can do this one of two ways: either select before the join so it is more explicit, or just join on the full tables.
    The end result is the same, it's mostly a readibility question.
    '''
    # order_detail = session.table("RAW_POS.ORDER_DETAIL")
    # order_header = session.table("RAW_POS.ORDER_HEADER")
    # truck = session.table("RAW_POS.TRUCK")
    # menu = session.table("RAW_POS.MENU")
    # franchise = session.table("RAW_POS.FRANCHISE")
    # location = session.table("RAW_POS.LOCATION")

    t_with_f = truck.join(franchise, truck['FRANCHISE_ID'] == franchise['FRANCHISE_ID'], rsuffix='_f')
    oh_w_t_and_l = order_header.join(t_with_f, order_header['TRUCK_ID'] == t_with_f['TRUCK_ID'], rsuffix='_t') \
                                .join(location, order_header['LOCATION_ID'] == location['LOCATION_ID'], rsuffix='_l')
    final_df = order_detail.join(oh_w_t_and_l, order_detail['ORDER_ID'] == oh_w_t_and_l['ORDER_ID'], rsuffix='_oh') \
                            .join(menu, order_detail['MENU_ITEM_ID'] == menu['MENU_ITEM_ID'], rsuffix='_m')
    final_df = final_df.select(F.col("ORDER_ID"), \
                            F.col("TRUCK_ID"), \
                            F.col("ORDER_TS"), \
                            F.col('ORDER_TS_DATE'), \
                            F.col("ORDER_DETAIL_ID"), \
                            F.col("LINE_NUMBER"), \
                            F.col("TRUCK_BRAND_NAME"), \
                            F.col("MENU_TYPE"), \
                            F.col("PRIMARY_CITY"), \
                            F.col("REGION"), \
                            F.col("COUNTRY"), \
                            F.col("FRANCHISE_FLAG"), \
                            F.col("FRANCHISE_ID"), \
                            F.col("FRANCHISEE_FIRST_NAME"), \
                            F.col("FRANCHISEE_LAST_NAME"), \
                            F.col("LOCATION_ID"), \
                            F.col("MENU_ITEM_ID"), \
                            F.col("MENU_ITEM_NAME"), \
                            F.col("QUANTITY"), \
                            F.col("UNIT_PRICE"), \
                            F.col("PRICE"), \
                            F.col("ORDER_AMOUNT"), \
                            F.col("ORDER_TAX_AMOUNT"), \
                            F.col("ORDER_DISCOUNT_AMOUNT"), \
                            F.col("ORDER_TOTAL"))
    final_df.create_or_replace_view('POS_FLATTENED_V')

def create_pos_view_stream(session):
    session.use_schema('HARMONIZED')
    _ = session.sql('CREATE OR REPLACE STREAM POS_FLATTENED_V_STREAM \
                        ON VIEW POS_FLATTENED_V \
                        SHOW_INITIAL_ROWS = TRUE').collect()

def test_pos_view(session):
    session.use_schema('HARMONIZED')
    tv = session.table('POS_FLATTENED_V')
    tv.limit(5).show()


# For local debugging
if __name__ == "__main__":
    # Add the utils package to our path and import the snowpark_utils function
    import os, sys
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)

    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()

    create_pos_view(session)
    create_pos_view_stream(session)
#    test_pos_view(session)

    session.close()
