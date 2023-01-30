#------------------------------------------------------------------------------
# Hands-On Lab: Data Engineering with Snowpark
# Script:       07_daily_city_metrics_process_sp/app.py
# Author:       Jeremiah Hansen, Caleb Baechtold
# Last Updated: 1/9/2023
#------------------------------------------------------------------------------

import time
from snowflake.snowpark import Session
import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F


def table_exists(session, schema='', name=''):
    exists = session.sql("SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}') AS TABLE_EXISTS".format(schema, name)).collect()[0]['TABLE_EXISTS']
    return exists

def create_daily_city_metrics_table(session):
    SHARED_COLUMNS= [T.StructField("DATE", T.DateType()),
                                        T.StructField("CITY_NAME", T.StringType()),
                                        T.StructField("COUNTRY_DESC", T.StringType()),
                                        T.StructField("DAILY_SALES", T.StringType()),
                                        T.StructField("AVG_TEMPERATURE_FAHRENHEIT", T.DecimalType()),
                                        T.StructField("AVG_TEMPERATURE_CELSIUS", T.DecimalType()),
                                        T.StructField("AVG_PRECIPITATION_INCHES", T.DecimalType()),
                                        T.StructField("AVG_PRECIPITATION_MILLIMETERS", T.DecimalType()),
                                        T.StructField("MAX_WIND_SPEED_100M_MPH", T.DecimalType()),
                                    ]
    DAILY_CITY_METRICS_COLUMNS = [*SHARED_COLUMNS, T.StructField("META_UPDATED_AT", T.TimestampType())]
    DAILY_CITY_METRICS_SCHEMA = T.StructType(DAILY_CITY_METRICS_COLUMNS)

    dcm = session.create_dataframe([[None]*len(DAILY_CITY_METRICS_SCHEMA.names)], schema=DAILY_CITY_METRICS_SCHEMA) \
                        .na.drop() \
                        .write.mode('overwrite').save_as_table('ANALYTICS.DAILY_CITY_METRICS')
    dcm = session.table('ANALYTICS.DAILY_CITY_METRICS')


def merge_daily_city_metrics(session):
    _ = session.sql('ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XLARGE').collect()
    time.sleep(5)

    print("{} records in stream".format(session.table('HARMONIZED.ORDERS_STREAM').count()))
    orders_stream_dates = session.table('HARMONIZED.ORDERS_STREAM').select(F.col("ORDER_TS_DATE").alias("DATE")).distinct()
    orders_stream_dates.limit(5).show()

    orders = session.table("HARMONIZED.ORDERS_STREAM").group_by(F.col('ORDER_TS_DATE'), F.col('PRIMARY_CITY'), F.col('COUNTRY')) \
                                        .agg(F.sum(F.col("PRICE")).as_("price_nulls")) \
                                        .with_column("DAILY_SALES", F.call_builtin("ZEROIFNULL", F.col("price_nulls"))) \
                                        .select(F.col('ORDER_TS_DATE').alias("DATE"), F.col("PRIMARY_CITY").alias("CITY_NAME"), \
                                        F.col("COUNTRY").alias("COUNTRY_DESC"), F.col("DAILY_SALES"))
#    orders.limit(5).show()

    weather_pc = session.table("FROSTBYTE_WEATHERSOURCE.ONPOINT_ID.POSTAL_CODES")
    countries = session.table("RAW_POS.COUNTRY")
    weather = session.table("FROSTBYTE_WEATHERSOURCE.ONPOINT_ID.HISTORY_DAY")
    weather = weather.join(weather_pc, (weather['POSTAL_CODE'] == weather_pc['POSTAL_CODE']) & (weather['COUNTRY'] == weather_pc['COUNTRY']), rsuffix='_pc')
    weather = weather.join(countries, (weather['COUNTRY'] == countries['ISO_COUNTRY']) & (weather['CITY_NAME'] == countries['CITY']), rsuffix='_c')
    weather = weather.join(orders_stream_dates, weather['DATE_VALID_STD'] == orders_stream_dates['DATE'])

    weather_agg = weather.group_by(F.col('DATE_VALID_STD'), F.col('CITY_NAME'), F.col('COUNTRY_C')) \
                        .agg( \
                            F.avg('AVG_TEMPERATURE_AIR_2M_F').alias("AVG_TEMPERATURE_F"), \
                            F.avg(F.call_udf("ANALYTICS.FAHRENHEIT_TO_CELSIUS_UDF", F.col("AVG_TEMPERATURE_AIR_2M_F"))).alias("AVG_TEMPERATURE_C"), \
                            F.avg("TOT_PRECIPiTATION_IN").alias("AVG_PRECIPITATION_IN"), \
                            F.avg(F.call_udf("ANALYTICS.INCH_TO_MILLIMETER_UDF", F.col("TOT_PRECIPiTATION_IN"))).alias("AVG_PRECIPITATION_MM"), \
                            F.max(F.col("MAX_WIND_SPEED_100M_MPH")).alias("MAX_WIND_SPEED_100M_MPH") \
                        ) \
                        .select(F.col("DATE_VALID_STD").alias("DATE"), F.col("CITY_NAME"), F.col("COUNTRY_C").alias("COUNTRY_DESC"), \
                            F.round(F.col("AVG_TEMPERATURE_F"), 2).alias("AVG_TEMPERATURE_FAHRENHEIT"), \
                            F.round(F.col("AVG_TEMPERATURE_C"), 2).alias("AVG_TEMPERATURE_CELSIUS"), \
                            F.round(F.col("AVG_PRECIPITATION_IN"), 2).alias("AVG_PRECIPITATION_INCHES"), \
                            F.round(F.col("AVG_PRECIPITATION_MM"), 2).alias("AVG_PRECIPITATION_MILLIMETERS"), \
                            F.col("MAX_WIND_SPEED_100M_MPH")
                            )
#    weather_agg.limit(5).show()

    daily_city_metrics_stg = orders.join(weather_agg, (orders['DATE'] == weather_agg['DATE']) & (orders['CITY_NAME'] == weather_agg['CITY_NAME']) & (orders['COUNTRY_DESC'] == weather_agg['COUNTRY_DESC']), \
                        how='left', rsuffix='_w') \
                    .select("DATE", "CITY_NAME", "COUNTRY_DESC", "DAILY_SALES", \
                        "AVG_TEMPERATURE_FAHRENHEIT", "AVG_TEMPERATURE_CELSIUS", \
                        "AVG_PRECIPITATION_INCHES", "AVG_PRECIPITATION_MILLIMETERS", \
                        "MAX_WIND_SPEED_100M_MPH")
#    daily_city_metrics_stg.limit(5).show()

    cols_to_update = {c: daily_city_metrics_stg[c] for c in daily_city_metrics_stg.schema.names}
    metadata_col_to_update = {"META_UPDATED_AT": F.current_timestamp()}
    updates = {**cols_to_update, **metadata_col_to_update}

    dcm = session.table('ANALYTICS.DAILY_CITY_METRICS')
    dcm.merge(daily_city_metrics_stg, (dcm['DATE'] == daily_city_metrics_stg['DATE']) & (dcm['CITY_NAME'] == daily_city_metrics_stg['CITY_NAME']) & (dcm['COUNTRY_DESC'] == daily_city_metrics_stg['COUNTRY_DESC']), \
                        [F.when_matched().update(updates), F.when_not_matched().insert(updates)])

    _ = session.sql('ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XSMALL').collect()

def main(session: Session) -> str:
    # Create the DAILY_CITY_METRICS table if it doesn't exist
    if not table_exists(session, schema='ANALYTICS', name='DAILY_CITY_METRICS'):
        create_daily_city_metrics_table(session)
    
    merge_daily_city_metrics(session)
#    session.table('ANALYTICS.DAILY_CITY_METRICS').limit(5).show()

    return f"Successfully processed DAILY_CITY_METRICS"


# For local debugging
# Be aware you may need to type-convert arguments if you add input parameters
if __name__ == '__main__':
    # Add the utils package to our path and import the snowpark_utils function
    import os, sys
    current_dir = os.getcwd()
    parent_parent_dir = os.path.dirname(os.path.dirname(current_dir))
    sys.path.append(parent_parent_dir)

    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()

    if len(sys.argv) > 1:
        print(main(session, *sys.argv[1:]))  # type: ignore
    else:
        print(main(session))  # type: ignore

    session.close()
