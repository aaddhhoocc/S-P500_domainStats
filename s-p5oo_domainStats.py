import os
import streamlit as st
import snowflake.connector
import pandas as pd

# Snowflake connection parameters
SNOWFLAKE_ACCOUNT=os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER=os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD=os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_DATABASE=os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA=os.getenv('SNOWFLAKE_SCHEMA')

#SNOWFLAKE_ACCOUNT
#SNOWFLAKE_USER
#SNOWFLAKE_PASSWORD
#SNOWFLAKE_DATABASE
#SNOWFLAKE_SCHEMA

# Connect to Snowflake
conn = snowflake.connector.connect(
    account=SNOWFLAKE_ACCOUNT,
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA
)

def query_ticker_list():
    with conn.cursor() as cursor:
        cursor.execute(f"""
                            select TICKER, COMPANY_NAME
                            from stats.sources.sp500
                            group by 1, 2
                            order by 1, 2
                        """)
        results = cursor.fetchall()
    return results

def query_ticker_stats(ticker):
    with conn.cursor() as cursor:
        cursor.execute(f"""
                            select 
                                date::DATE, 
                                desktop_visits::INT, 
                                mobile_visits::INT, 
                                total_visits::INT
                            from stats.sources.sp500
                            where ticker = '{ticker}'
                            order by date DESC
                            limit 30
                        """)
        results = cursor.fetchall()
    return results
    
st.title('S&P 500 Domain Visit Stats')

results = query_ticker_list()

if results:
    ticker_df = pd.DataFrame(results, columns=['TICKER', 'COMPANY_NAME'])
    ticker_selectbox = st.selectbox("Select a S&P ticker", ticker_df['TICKER']+' | '+ticker_df['COMPANY_NAME'])
    results = query_ticker_stats(ticker_selectbox.split('|')[0].strip())
    if results:
        stats_df = pd.DataFrame(results, columns=['date', 'desktop_visits', 'mobile_visits', 'total_visits'])
        st.line_chart(stats_df.set_index('date'))
else:
    st.write('No data found for the selected S&P domain')
