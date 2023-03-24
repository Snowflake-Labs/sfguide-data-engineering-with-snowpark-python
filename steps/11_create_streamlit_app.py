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
            , round(avg(daily_sales) 
                over (partition by city_name 
                    order by date rows between 6 preceding and current row),0
                        ) as "7_DAY_AVG_SALES"
            , iff(precip_inches > 0, 'Y', 'N') as precip_ind
        from HOL_DB.ANALYTICS.DAILY_CITY_METRICS
    )
    , final as(
        select
            actual.city_name
            , actual.date
            , actual.month
            , actual.year
            , actual.daily_sales
            , actual."7_DAY_AVG_SALES"
            , actual.daily_sales - actual."7_DAY_AVG_SALES" as sales_diff_to_avg
            , actual.temp_fahrenheit
            , monthly_avg.monthly_avg_temp_fahrenheit
            , actual.temp_fahrenheit - monthly_avg.monthly_avg_temp_fahrenheit as temp_fahrenheit_diff
            , actual.precip_inches
            , monthly_avg.monthly_avg_precip_inches
            , actual.precip_ind
            , actual.precip_inches - monthly_avg.monthly_avg_precip_inches as precip_diff
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

# Set the page title
st.set_page_config(page_title='Sales Metrics')

# Create a Streamlit app
def main():

    # Add a title to the app
    st.title('Sales Metrics')

    # Fetch data from Snowflake
    df = get_data()

    # Filter by city name using a sidebar widget
    # city_name = st.sidebar.selectbox('Select a city:', sorted(df['CITY_NAME'].unique()))
    city_name = st.sidebar.selectbox('Select a city:', sorted(df['CITY_NAME'].unique()), index=sorted(df['CITY_NAME'].unique()).index('Boston'))
    # month = st.sidebar.selectbox('Select a month:', range(1, 12))
    month = st.sidebar.multiselect('Select month(s):', sorted(df['MONTH'].unique()), default=[1], key='month_filter')
    # year = st.sidebar.selectbox('Select a year:')
    year = st.sidebar.multiselect('Select year(s):', sorted(df['YEAR'].unique()), default=[2022], key='year_filter')

    # Apply the city name filter
    df = df[df['CITY_NAME'] == city_name]
    df = df[df['MONTH'].isin(month)]
    df = df[df['YEAR'].isin(year)]

    # Chart Title
    st.subheader('Does Temperature Impact Sales?')

    # Create a chart using Altair
    chart = alt.Chart(df).mark_circle().encode(
        x='TEMP_FAHRENHEIT',
        y='DAILY_SALES',
        # color='7_DAY_AVG_SALES',
        # size='DAILY_SALES'
    ).interactive()
    
    # Display the chart
    st.altair_chart(chart, use_container_width=True)

    # Chart Title
    st.subheader('Does Precipitation Impact Sales?')

    # Create a chart using Altair
    chart = alt.Chart(df).mark_circle().encode(
        x='PRECIP_INCHES',
        y='DAILY_SALES',
        # color='7_DAY_AVG_SALES',
        # size='DAILY_SALES'
    ).interactive()

    # Display the chart
    st.altair_chart(chart, use_container_width=True)

    # Chart Title
    st.subheader('Is there Any Correlation?')
 
    # plotting correlation heatmap
    dataplot = sb.heatmap(df.corr(), cmap="YlGnBu", annot=True, cbar=False)

    # displaying heatmap
    st.pyplot(dataplot.figure)
    st.set_option('deprecation.showPyplotGlobalUse', False)

    # Chart Title
    st.subheader('Temperature vs. Daily Sales')

    # Create a chart using Matplotlib
    fig, ax = plt.subplots()
    # Plot the 7-day average sales as a line plot
    ax.plot(df['DATE'], df['7_DAY_AVG_SALES'], color='g', label='7-Day Avg Sales')
    ax.scatter(df['DATE'], df['DAILY_SALES'], color='b', label='Daily Sales')
    ax.set_xlabel('Date')
    plt.xticks(rotation=70)
    ax.set_ylabel('Daily Sales')
    ax.legend()

    # Create a second y-axis for temperature
    ax2 = ax.twinx()
    ax2.plot(df['DATE'], df['TEMP_FAHRENHEIT'], color='r', label='Temp Fahrenheit')
    ax2.set_ylabel('Temperature (F)')

    # Display the chart
    st.pyplot(fig)

    # Chart Title
    st.subheader('Precip vs. Daily Sales')

    # Create a chart using Matplotlib
    fig, ax = plt.subplots()
    # Plot the 7-day average sales as a line plot
    ax.plot(df['DATE'], df['7_DAY_AVG_SALES'], color='g', label='7-Day Avg Sales')
    ax.scatter(df['DATE'], df['DAILY_SALES'], color='b', label='Daily Sales')
    ax.set_xlabel('Date')
    plt.xticks(rotation=70)
    ax.set_ylabel('Daily Sales')
    ax.legend()

    # Create a second y-axis for precipitation
    ax2 = ax.twinx()
    ax2.plot(df['DATE'], df['PRECIP_INCHES'], color='r', label='Precip Inches')
    ax2.set_ylabel('Precip (Inches)')

    # Display the chart
    st.pyplot(fig)

# Run the app
if __name__ == '__main__':
    main()