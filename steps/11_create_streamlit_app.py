import sys
sys.path.append('../')
from utils import snowpark_utils
from snowflake.snowpark import Session
import streamlit as st
import altair as alt
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sb

# Define function to fetch data from Snowflake
def get_data():

    # Define the query
    query = """
    with monthly_avg as(
        select 
            city_name
            , date_part(month, date) as month
            , round(avg(avg_temperature_fahrenheit),0) as monthly_avg_temp_fahrenheit
            , round(avg(avg_precipitation_inches),0) as monthly_avg_precip_inches
        from HOL_DB.ANALYTICS.DAILY_CITY_METRICS
        group by 1,2
        order by 1,2
    )
    , actual as(
        select
            city_name
            , date
            , date_part(month, date) as month
            , date_part(year, date) as year
            , round(daily_sales,0) as daily_sales
            , avg_temperature_fahrenheit as temp_fahrenheit
            , avg_precipitation_inches as precip_inches
            , iff(precip_inches > 0, '🌧️', '☀️') as precip_ind
        from HOL_DB.ANALYTICS.DAILY_CITY_METRICS
    )
    , final as(
        select
            actual.city_name
            , actual.date
            , actual.month
            , actual.year
            , actual.daily_sales
            , actual.temp_fahrenheit
            , monthly_avg.monthly_avg_temp_fahrenheit
            , actual.precip_inches
            , monthly_avg.monthly_avg_precip_inches
            , actual.precip_ind
        from actual
        inner join monthly_avg
            on actual.city_name = monthly_avg.city_name
            and actual.month = monthly_avg.month
    )
    select
        *
    from final 
    order by 1,2
    """

    # session=create_session_object()
    session=snowpark_utils.get_snowpark_session()

    # Execute the query and load the results into a DataFrame
    df = session.sql(query).to_pandas()

    # Return the DataFrame
    return df

# Set the page title and layout
st.set_page_config(page_title='Sales Metrics', layout='wide')


# Create a Streamlit app
def main():

    # Fetch data from Snowflake
    df = get_data()

    # Filter by city name and year using a sidebar widget
    city_name = st.sidebar.selectbox('Select a city:', sorted(df['CITY_NAME'].unique()), index=sorted(df['CITY_NAME'].unique()).index('Boston'))
    month = st.sidebar.multiselect('Select month(s):', sorted(df['MONTH'].unique()), default=[1,2,3,4,5,6,7,8,9,10,11,12], key='month_filter')
    year = st.sidebar.multiselect('Select year(s):', sorted(df['YEAR'].unique()), default=[2022], key='year_filter')

    # Apply filters to df
    mask = ((df['CITY_NAME'] == city_name) & (df['MONTH'].isin(month)) & (df['YEAR'].isin(year)))
    filtered_df = df[mask]

    # Add a title to the app
    st.title(f'{city_name} Sales Metrics')

    # create two columns to hold the daily sales scatter and box plots
    col1, col2 = st.columns(2)

    # SALES v TEMPERATURE SCATTER PLOT
    with col1:
        st.subheader('Does Temperature Impact Sales?')

        s_v_t_chart = alt.Chart(filtered_df).mark_circle().encode(
            x=alt.X('TEMP_FAHRENHEIT', title='Temperature (°F)'),
            y=alt.Y('DAILY_SALES', title='Daily Sales')
        ).interactive()
        
        # Display the chart
        st.altair_chart(s_v_t_chart, use_container_width=True)

    # SALES v PRECIPITATION BOX PLOT
    with col2:
        st.subheader('Does Precipitation Impact Sales?')

        p_v_s_chart = alt.Chart(filtered_df).mark_boxplot(extent='min-max').encode(
            x=alt.X('PRECIP_IND:O', title='Precipitation', axis=alt.Axis(labelAngle=0)), 
            y=alt.Y('DAILY_SALES:Q', title='Daily Sales')
        ).interactive()

        # Display the chart
        st.altair_chart(p_v_s_chart, use_container_width=True)

    # create two tabs to switch between the two time series graphs
    st.subheader('Time Series Analysis')
    tab1, tab2 = st.tabs(['Sales vs Temperature over Time', 'Sales vs Preciptation over Time'])

    # create a base chart with the month-year as the x-axis
    base = alt.Chart(filtered_df).encode(
        alt.X('yearmonth(DATE):T', axis=alt.Axis(title=None))
    )

    # add average daily sales per month-year as the first y-axis
    sales_line = base.mark_line(interpolate='monotone').encode(
        alt.Y('mean(DAILY_SALES)',
              axis=alt.Axis(title='Avg. Daily Sales', titleColor='#5276A7'))
    )
    
    with tab1:
        # AVERAGE MONTHLY SALES v AVERAGE MONTHLY TEMPERATURE
        # add average temperature per month-year as the second y-axis
        temp_line = base.mark_line(color='#57A44C').encode(
            alt.Y('mean(TEMP_FAHRENHEIT)',
                axis=alt.Axis(title='Avg. Temperature (°F)', titleColor='#57A44C'))
        )
        sales_v_temp_chart = alt.layer(sales_line, temp_line).resolve_scale(
            y = 'independent'
        ).interactive()
        st.altair_chart(sales_v_temp_chart, use_container_width=True)
        
    with tab2:
        # AVERAGE MONTHLY SALES v TOTAL MONTHLY PRECIPITATION
        # add total preciptation per month-year as the second y-axis
        precip_line = base.mark_line(color='#57A44C').encode(
            alt.Y('mean(PRECIP_INCHES):Q', 
                axis=alt.Axis(title='Avg. Precipitation', titleColor='#57A44C'))
        )
        sales_v_precip_chart = alt.layer(sales_line, precip_line).resolve_scale(
            y = 'independent'
        ).interactive()
        st.altair_chart(sales_v_precip_chart, use_container_width=True)

    # CORRELATION MATRIX
    st.subheader('Is there Any Correlation?')
 
    corr_df = filtered_df[['DAILY_SALES', 'PRECIP_INCHES', 'TEMP_FAHRENHEIT', 'MONTHLY_AVG_PRECIP_INCHES', 'MONTHLY_AVG_TEMP_FAHRENHEIT']]
    corr_df = corr_df.rename(columns={
        'DAILY_SALES': 'Daily Sales',
        'PRECIP_INCHES': 'Precip. (in)',
        'TEMP_FAHRENHEIT': 'Temp. (°F)',
        'MONTHLY_AVG_PRECIP_INCHES': 'Monthly Avg. Precip. (in)',
        'MONTHLY_AVG_TEMP_FAHRENHEIT': 'Monthly Avg. Temp. (°F)'
    })
    corrMatrix = corr_df.corr().reset_index().melt('index')
    corrMatrix.columns = ['var1', 'var2', 'correlation']

    base = alt.Chart(corrMatrix).transform_filter(
        alt.datum.var1 < alt.datum.var2
    ).encode(
        x=alt.X('var1:O', axis=alt.Axis(labelLimit=300)),
        y=alt.Y('var2:O', axis=alt.Axis(labelLimit=300))
    ).properties(
        width=alt.Step(100),
        height=alt.Step(100)
    )

    rects = base.mark_rect().encode(
        color='correlation:Q'
    )

    text = base.mark_text(
        size=30
    ).encode(
        text=alt.Text('correlation', format=".2f"),
        color=alt.condition(
            "datum.correlation > 0.5",
            alt.value('white'),
            alt.value('black')
        )
    )

    st.altair_chart(rects + text, use_container_width=False)

    # st.checkbox value defaults to False
    show_df = st.checkbox('Show raw data')

    # show dataframe is checkbox selected
    if show_df:
        st.dataframe(filtered_df, use_container_width=True)

# Run the app
if __name__ == '__main__':
    main()
