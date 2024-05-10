from matplotlib import pyplot as plt
import streamlit as st
import pandas as pd
import sqlite3
import altair as alt
import streamlit as st
from datetime import datetime, timedelta


st.set_page_config(page_title="Crypto Compass",page_icon="🧭",layout="wide")

hide_full_screen = '''
<style>
button[title="View fullscreen"]{
    visibility: hidden;}
</style>
'''

st.markdown(hide_full_screen, unsafe_allow_html=True) 

st.sidebar.image('/home/entukio/projects/scrapper_crypto_top/files/logo.png')


def stringify_value(value):
    if abs(value) >= 1e9:
        return '{:.2f} b'.format(value / 1e9)
    elif abs(value) >= 1e6:
        return '{:.1f} m'.format(value / 1e6)
    elif abs(value) >= 1e3:
        return '{:.0f} k'.format(value / 1e3)
    else:
        return str(value)

conn=sqlite3.connect('/home/entukio/projects/scrapper_crypto_top/files/top_500_with_Mcap_stablecoins_excluded.db')

df=pd.read_sql_query('SELECT * FROM top_500_with_Mcap_stablecoins_excluded',conn)

scraping_date = df['Date'][0]
scraping_date = (scraping_date) + ' UTC'
df.drop(columns=['Date'],inplace=True)

############################### ALTAIR BASED ON IRIS
#excluding some
#df_final = df[~df['Id'].isin(['Bitcoin_BTC', 'Ethereum_ETH'])]

# podział danych na 4 kwartyle wg. marketcap

df_final = df.copy()

df_final['current_Middle_Trend_Up'] = df_final['current_Middle_Trend_Up'].apply(lambda x: int.from_bytes(x, byteorder='big') if x is not None else None)
df_final['current_Long_Trend_Up'] = df_final['current_Long_Trend_Up'].apply(lambda x: int.from_bytes(x, byteorder='big') if x is not None else None)

df_final['current_Middle_Trend_Up'] = df_final['current_Middle_Trend_Up'].apply(lambda x: bool(x) if x is not None else None)
df_final['current_Long_Trend_Up'] = df_final['current_Long_Trend_Up'].apply(lambda x: bool(x) if x is not None else None)

df_final['quartile'] = pd.qcut(df_final['Mcap'], 4, labels=False)

df_1q = df_final[df_final['quartile'] == 0]
q1_smallest = df_1q.nsmallest(1,'Mcap')
q1_largest = df_1q.nlargest(1,'Mcap')

df_2q = df_final[df_final['quartile'] == 1]
q2_smallest = df_2q.nsmallest(1,'Mcap')
q2_largest = df_2q.nlargest(1,'Mcap')

df_3q = df_final[df_final['quartile'] == 2]
q3_smallest = df_3q.nsmallest(1,'Mcap')
q3_largest = df_3q.nlargest(1,'Mcap')

df_4q = df_final[df_final['quartile'] == 3]
q4_smallest = df_4q.nsmallest(1,'Mcap')
q4_largest = df_4q.nlargest(1,'Mcap')

df_final.drop(columns=['quartile'],inplace=True)

def replace_boolean_with_icons(value):
    if isinstance(value, bool):
        return "🟢" if value else "🔴"  # Custom icons representing True and False
    return value

df_final_bool = df_final.copy()

# Apply the function to the DataFrame
df_final = df_final.applymap(replace_boolean_with_icons)

desired_columns = ['Id','Name','Symbol','current_Middle_Trend_Up',
       'current_Long_Trend_Up','middle_flip_date', 'long_flip_date', 'Price', 'Mcap', 'Vol24h', 'From_ath', 'Rank',
       'rank_1dΔ', 'rank_3dΔ', 'rank_7dΔ', 'rank_14dΔ', 'mcap_1dΔ', 'mcap_3dΔ',
       'mcap_7dΔ', 'mcap_14dΔ']

# Reassign the DataFrame with the desired column order
df_final = df_final[desired_columns]

df_final = df_final.sort_values(by='middle_flip_date', ascending=False).copy()


def show_Market_Caps():

    st.write('# Charts')

    ############## 1 day change #############

    st.write(f"""
    # Top 400 Market cap 1 day change compared to total market cap
    Quartile 1: the lowest 25% tokens with value:
    {(q1_smallest['Mcap']).apply(stringify_value).values[0]} - {(q1_largest['Mcap']).apply(stringify_value).values[0]}
            """)

    mcap_map_q1 = alt.Chart(df_1q).mark_point().encode(
        x=alt.X('Mcap', axis=alt.Axis(format=',d')),
        y=alt.Y('mcap_1dΔ', axis=alt.Axis(format=',d')),
        color='Name'
    )

    st.altair_chart(mcap_map_q1,use_container_width=True)

    #2 q

    st.write(f"""
    Quartile 2:
    {(q2_smallest['Mcap']).apply(stringify_value).values[0]} - {(q2_largest['Mcap']).apply(stringify_value).values[0]}
            """)

    mcap_map_q2 = alt.Chart(df_2q).mark_point().encode(
        x=alt.X('Mcap', axis=alt.Axis(format=',d')),
        y=alt.Y('mcap_1dΔ', axis=alt.Axis(format=',d')),
        color='Name'
    )

    st.altair_chart(mcap_map_q2,use_container_width=True)

    #3 q

    st.write(f"""
    Quartile 3:
    {(q3_smallest['Mcap']).apply(stringify_value).values[0]} - {(q3_largest['Mcap']).apply(stringify_value).values[0]}
            """)

    mcap_map_q3 = alt.Chart(df_3q).mark_point().encode(
        x=alt.X('Mcap', axis=alt.Axis(format=',d')),
        y=alt.Y('mcap_1dΔ', axis=alt.Axis(format=',d')),
        color='Name'
    )

    st.altair_chart(mcap_map_q3,use_container_width=True)

    #4 q

    st.write(f"""
    Quartile 4:
    {(q4_smallest['Mcap']).apply(stringify_value).values[0]} - {(q4_largest['Mcap']).apply(stringify_value).values[0]}
            """)

    mcap_map_q4 = alt.Chart(df_4q).mark_point().encode(
        x=alt.X('Mcap', axis=alt.Axis(format=',d')),
        y=alt.Y('mcap_1dΔ', axis=alt.Axis(format=',d')),
        color='Name'
    )



    st.altair_chart(mcap_map_q4,use_container_width=True)

    ############## 7 day change #############

    st.write(f"""
    # Top 400 Market cap 7 day change compared to total market cap
    Quartile 1: the lowest 25% tokens with value:
    {(q1_smallest['Mcap']).apply(stringify_value).values[0]} - {(q1_largest['Mcap']).apply(stringify_value).values[0]}
            """)

    mcap_map_q1 = alt.Chart(df_1q).mark_point().encode(
        x=alt.X('Mcap', axis=alt.Axis(format=',d')),
        y=alt.Y('mcap_7dΔ', axis=alt.Axis(format=',d')),
        color='Name'
    )

    st.altair_chart(mcap_map_q1,use_container_width=True)

    #2 q

    st.write(f"""
    Quartile 2:
    {(q2_smallest['Mcap']).apply(stringify_value).values[0]} - {(q2_largest['Mcap']).apply(stringify_value).values[0]}
            """)

    mcap_map_q2 = alt.Chart(df_2q).mark_point().encode(
        x=alt.X('Mcap', axis=alt.Axis(format=',d')),
        y=alt.Y('mcap_7dΔ', axis=alt.Axis(format=',d')),
        color='Name'
    )

    st.altair_chart(mcap_map_q2,use_container_width=True)

    #3 q

    st.write(f"""
    Quartile 3:
    {(q3_smallest['Mcap']).apply(stringify_value).values[0]} - {(q3_largest['Mcap']).apply(stringify_value).values[0]}
            """)

    mcap_map_q3 = alt.Chart(df_3q).mark_point().encode(
        x=alt.X('Mcap', axis=alt.Axis(format=',d')),
        y=alt.Y('mcap_7dΔ', axis=alt.Axis(format=',d')),
        color='Name'
    )

    st.altair_chart(mcap_map_q3,use_container_width=True)

    #4 q

    st.write(f"""
    Quartile 4:
    {(q4_smallest['Mcap']).apply(stringify_value).values[0]} - {(q4_largest['Mcap']).apply(stringify_value).values[0]}
            """)

    mcap_map_q4 = alt.Chart(df_4q).mark_point().encode(
        x=alt.X('Mcap', axis=alt.Axis(format=',d')),
        y=alt.Y('mcap_7dΔ', axis=alt.Axis(format=',d')),
        color='Name'
    )



    st.altair_chart(mcap_map_q4,use_container_width=True)



def show_Trends(df_trends):

    st.header(f'''
    ## Fresh Up Trends
    Last update: {scraping_date}
            ''')
    
    st.write(f'''
    #### New Uptrends from last 3 days
            ''')
    
    with st.expander("Info"):
        # Content inside the expandable panel
        st.write('''The chart shows the change rank by market cap in USD and by the market cap.
                '''
                            )

   
    # plot dfs


    num_charts = len(last_3d_trends_df)

    for i in range(0, num_charts, 3):
        element = last_3d_trends_df.iloc[i].copy()
        element = pd.DataFrame(element).copy()
        element = element.T.copy()
        element_show = element.drop(columns=['Id']).copy()
        element_show['Trend Start'] = pd.to_datetime(element_show['Trend Start']).dt.date
        element_show_more = element_show[['Price','Market Cap','ATH Δ','rank_1dΔ','rank_3dΔ']].copy()
        element_show.drop(columns = ['Price','Market Cap','ATH Δ','rank_3dΔ','rank_1dΔ','rank_7dΔ','rank_14dΔ','Vol24h','rank_1dΔ','rank_7dΔ','rank_14dΔ','mcap_1dΔ','mcap_3dΔ','mcap_7dΔ','mcap_14dΔ'],inplace=True)
                
        conn = sqlite3.connect(f"/home/entukio/projects/scrapper_crypto_top/files/prices/{element.iloc[0]['Id']}.db")
        coin_db = pd.read_sql(f"SELECT * FROM db_{(element.iloc[0]['Name']).split(' ')[-1]}USD",conn)
        conn.close()

        coin_db = coin_db.tail(5)
        coin_db['Date'] = pd.to_datetime(coin_db['Date'])
     

        if i + 1 < num_charts:
            element_1 = last_3d_trends_df.iloc[i+1].copy()
            element_1 = pd.DataFrame(element_1).copy()
            element_1 = element_1.T.copy()
            element_show_1 = element_1.drop(columns=['Id']).copy()
            element_show_1['Trend Start'] = pd.to_datetime(element_show_1['Trend Start']).dt.date
            element_show_1_more = element_show_1[['Price','Market Cap','ATH Δ','rank_1dΔ','rank_3dΔ']].copy()
            element_show_1.drop(columns = ['Price','Market Cap','ATH Δ','rank_3dΔ','rank_1dΔ','rank_7dΔ','rank_14dΔ','Vol24h','rank_1dΔ','rank_7dΔ','rank_14dΔ','mcap_1dΔ','mcap_3dΔ','mcap_7dΔ','mcap_14dΔ'],inplace=True)


                    
            conn = sqlite3.connect(f"/home/entukio/projects/scrapper_crypto_top/files/prices/{element_1.iloc[0]['Id']}.db")
            coin_db_1 = pd.read_sql(f"SELECT * FROM db_{(element_1.iloc[0]['Name']).split(' ')[-1]}USD",conn)
            conn.close()
            coin_db_1 = coin_db_1.tail(5)
            coin_db_1['Date'] = pd.to_datetime(coin_db_1['Date'])
        
        if i + 2 < num_charts:
            element_2 = last_3d_trends_df.iloc[i+2].copy()
            element_2 = pd.DataFrame(element_2).copy()
            element_2 = element_2.T.copy()
            element_show_2 = element_2.drop(columns=['Id']).copy()
            element_show_2['Trend Start'] = pd.to_datetime(element_show_2['Trend Start']).dt.date
            element_show_2_more = element_show_2[['Price','Market Cap','ATH Δ','rank_1dΔ','rank_3dΔ']].copy()
            element_show_2.drop(columns = ['Price','Market Cap','ATH Δ','rank_3dΔ','rank_1dΔ','rank_7dΔ','rank_14dΔ','Vol24h','rank_1dΔ','rank_7dΔ','rank_14dΔ','mcap_1dΔ','mcap_3dΔ','mcap_7dΔ','mcap_14dΔ'],inplace=True)


                    
            conn = sqlite3.connect(f"/home/entukio/projects/scrapper_crypto_top/files/prices/{element_2.iloc[0]['Id']}.db")
            coin_db_2 = pd.read_sql(f"SELECT * FROM db_{(element_2.iloc[0]['Name']).split(' ')[-1]}USD",conn)
            conn.close()
            coin_db_2 = coin_db_2.tail(5)
            coin_db_2['Date'] = pd.to_datetime(coin_db_2['Date'])

        chart_width = 600
        chart_height = 300

        chart_color_range = ['#e1a100', '#bdbdbd', '#656565', '#02808b']

        col1, col2, col3 = st.columns(3)
        with col1:
            st.subheader(f"{element.iloc[0]['Name']}")
            st.dataframe(data=element_show,hide_index=True)
            st.dataframe(data=element_show_more,hide_index=True)
            # Plotting
            #######
            # Calculate y-axis limits
            y_min = coin_db[['Close', 'EMA23', 'EMA56', 'SMA200']].min().min() * 0.8
            y_max = coin_db[['Close', 'EMA23', 'EMA56', 'SMA200']].max().max() * 1.2

            # Melt the DataFrame to long format for easier plotting
            coin_db_long = coin_db.melt(id_vars='Date', var_name='Line', value_name='Value')

            coin_db_long = coin_db_long[coin_db_long['Line'].isin(['Close', 'EMA23', 'EMA56', 'SMA200'])].copy()

            # Create Altair chart
            chart = alt.Chart(coin_db_long).mark_line().encode(
                x=alt.X('Date:T', axis=alt.Axis(title='Date', format='%Y-%m-%d', grid=True, labelAngle=-90)),
                y=alt.Y('Value:Q', scale=alt.Scale(domain=[y_min, y_max])),
                color=alt.Color('Line:N', scale=alt.Scale(domain=['Close', 'EMA23', 'EMA56', 'SMA200'], range=chart_color_range))
            ).properties(
                width=chart_width,
                height=chart_height
            )

            # Display the chart using st.altair_chart
            st.altair_chart(chart, use_container_width=True)


        if i + 1 < num_charts:
            with col2:
                st.subheader(f"{element_1.iloc[0]['Name']}")
                st.dataframe(data=element_show_1,hide_index=True)
                st.dataframe(data=element_show_1_more,hide_index=True)
                # Plotting
                # Calculate y-axis limits
                y_min_1 = coin_db_1[['Close', 'EMA23', 'EMA56', 'SMA200']].min().min() * 0.8
                y_max_1 = coin_db_1[['Close', 'EMA23', 'EMA56', 'SMA200']].max().max() * 1.2

                # Melt the DataFrame to long format for easier plotting
                coin_db_long_1 = coin_db_1.melt(id_vars='Date', var_name='Line', value_name='Value')
                coin_db_long_1 = coin_db_long_1[coin_db_long_1['Line'].isin(['Close', 'EMA23', 'EMA56', 'SMA200'])].copy()

                # Create Altair chart
                chart_1 = alt.Chart(coin_db_long_1).mark_line().encode(
                    x=alt.X('Date:T', axis=alt.Axis(title='Date', format='%Y-%m-%d', grid=True, labelAngle=-90)),
                    y=alt.Y('Value:Q', scale=alt.Scale(domain=[y_min_1, y_max_1])),
                    color=alt.Color('Line:N', scale=alt.Scale(domain=['Close', 'EMA23', 'EMA56', 'SMA200'], range=chart_color_range))
                ).properties(
                    width=chart_width,
                    height=chart_height
                )

                # Display the chart using st.altair_chart
                st.altair_chart(chart_1, use_container_width=True)

        if i + 2 < num_charts:
            with col3:
                st.subheader(f"{element_2.iloc[0]['Name']}")
                st.dataframe(data=element_show_2,hide_index=True)
                st.dataframe(data=element_show_2_more,hide_index=True)
                # Plotting
                # Calculate y-axis limits
                y_min_2 = coin_db_2[['Close', 'EMA23', 'EMA56', 'SMA200']].min().min() * 0.8
                y_max_2 = coin_db_2[['Close', 'EMA23', 'EMA56', 'SMA200']].max().max() * 1.2

                # Melt the DataFrame to long format for easier plotting
                coin_db_long_2 = coin_db_2.melt(id_vars='Date', var_name='Line', value_name='Value')
                coin_db_long_2 = coin_db_long_2[coin_db_long_2['Line'].isin(['Close', 'EMA23', 'EMA56', 'SMA200'])].copy()

                # Create Altair chart
                chart_2 = alt.Chart(coin_db_long_2).mark_line().encode(
                    x=alt.X('Date:T', axis=alt.Axis(title='Date', format='%Y-%m-%d', grid=True, labelAngle=-90)),
                    y=alt.Y('Value:Q', scale=alt.Scale(domain=[y_min_2, y_max_2])),
                    color=alt.Color('Line:N', scale=alt.Scale(domain=['Close', 'EMA23', 'EMA56', 'SMA200'], range=chart_color_range))
                ).properties(
                    width=chart_width,
                    height=chart_height
                )

                # Display the chart using st.altair_chart
                st.altair_chart(chart_2, use_container_width=True)

        
        
    
    st.subheader('All Recent Trend Changes')
   
    with st.expander("Info"):
        # Content inside the expandable panel
        st.write('''The chart shows the change rank by market cap in USD and by the market cap.
                '''
                            )
    st.markdown(
    """
    <style>
    [data-testid="stElementToolbar"] {
        display: none;
    }
    </style>
    """,
    unsafe_allow_html=True
    )

    df_trends = df_trends.fillna('No data')

    st.dataframe(data=df_trends,hide_index=True,width=0)

def market_overview():

    st.header(f'''
    Market Overview
    Last update: {scraping_date}
            ''')
    


    col1,col2,col3,col4,col5,col6,col7,col8 = st.columns(8)
    with col1:
        st.write("Total Assets Tracked")
        st.subheader(Total_coins)
    with col2:
        st.write("In Uptrend")
        st.subheader(In_Uptrend_Perc)
    with col3:
        st.write("In Downtrend")
        st.subheader(In_Downtrend_Perc)
    with col4:
        st.write("No Trend Info")
        st.subheader(No_Trend_Perc)
    with col5:
        st.write("Above 200 MA")
        st.subheader(Above_200_MA_Perc)
    with col6:
        st.write("Below 200 MA")
        st.subheader(Below_200_MA_Perc)
    with col7:
        st.write("No MA Info")
        st.subheader(No_200_MA_info_Perc)
    with col8:
        st.write("Crypto Fear&Greed Index")
        st.subheader(Fear_Greed)

    st.markdown('---')

    col1_1,col1_2,col_1_3 = st.columns(3)
    with col1_1:
        source_trends = pd.DataFrame({"Trend Info": ['Uptrend','Downtrend','No trend data'], "value": [In_Uptrend,In_Downtrend,No_Trend_Info]})

        chart_trends = alt.Chart(source_trends).mark_arc(innerRadius=80).encode(
            theta="value",
            color=alt.Color(field="Trend Info", type="nominal", scale=alt.Scale(range=['#d10819','#6e6e6e','#0baa02'])), 
        )
        # Display the chart using st.altair_chart
        st.altair_chart(chart_trends, use_container_width=True)
    
        source_mas = pd.DataFrame({"Position of 200 MA": ['Above 200 MA','Below 200 MA','No 200 MA data'], "value": [Above_200_MA,Below_200_MA,No_200_MA_info]})

        chart_mas = alt.Chart(source_mas).mark_arc(innerRadius=80).encode(
            theta="value",
            color=alt.Color(field="Position of 200 MA", type="nominal", scale=alt.Scale(range=['#0baa02', '#d10819', '#6e6e6e'])), 
        )
        # Display the chart using st.altair_chart
        st.altair_chart(chart_mas, use_container_width=True)


page = st.sidebar.radio("Menu", ["Market Overview","Trends", "Market Caps"])

df_trends = df_final.copy()

df_trends = df_trends.rename(columns={'current_Middle_Trend_Up': 'Trend', 'current_Long_Trend_Up': 'Long Trend', 'middle_flip_date': 'Trend Start', 'long_flip_date': 'Long Trend Start','Mcap':'Market Cap','From_ath':'ATH Δ'})

df_trends['Name'] = df_trends['Name'] + ' ' + df_trends['Symbol']

df_trends.drop(columns=['Symbol'],inplace=True)

df_trends_display = df_trends.drop(columns=['Id']).copy()


df_cleared_for_charts = df_trends.dropna(subset=['Trend Start']).copy()

df_cleared_for_charts = df_cleared_for_charts[df_cleared_for_charts['Trend'] == '🟢']


# Convert the 'Trend Started' column to datetime
df_cleared_for_charts['Trend Start'] = pd.to_datetime(df_cleared_for_charts['Trend Start'])

# Get yesterday's date
yesterday = datetime.now() - timedelta(days=1)
yesterday_df = df_cleared_for_charts[df_cleared_for_charts['Trend Start'].dt.date == yesterday.date()].copy()

# Get two days ago's date
two_days_ago = datetime.now() - timedelta(days=2)
two_days_ago_df = df_cleared_for_charts[df_cleared_for_charts['Trend Start'].dt.date == two_days_ago.date()].copy()

# Get three days ago's date
three_days_ago = datetime.now() - timedelta(days=3)
three_days_ago_df = df_cleared_for_charts[df_cleared_for_charts['Trend Start'].dt.date == three_days_ago.date()].copy()

last_3d_trends_df = pd.concat([yesterday_df, two_days_ago_df, three_days_ago_df], axis=0)

### Summary stats ###

conn_overview=sqlite3.connect('/home/entukio/projects/scrapper_crypto_top/files/market_overview.db')

df_overview=pd.read_sql_query('SELECT * FROM market_overview',conn_overview)

Total_coins = df_overview.iloc[-1]['Total_coins']

In_Uptrend = df_overview.iloc[-1]['In_Uptrend']
In_Downtrend = df_overview.iloc[-1]['In_Downtrend']

No_Trend_Info = df_overview.iloc[-1]['No_Trend_Info']

Above_200_MA = df_overview.iloc[-1]['Above_200_MA']
No_200_MA_info = df_overview.iloc[-1]['No_200_MA_info']
Below_200_MA = df_overview.iloc[-1]['Below_200_MA']
Fear_Greed = df_overview.iloc[-1]['Fear_Greed']


In_Uptrend_Perc = df_overview.iloc[-1]['In_Uptrend_Perc']
In_Downtrend_Perc = df_overview.iloc[-1]['In_Downtrend_Perc']
No_Trend_Perc = df_overview.iloc[-1]['No_Trend_Perc']

Above_200_MA_Perc = df_overview.iloc[-1]['Above_200_MA_Perc']
Below_200_MA_Perc = df_overview.iloc[-1]['Below_200_MA_Perc']
No_200_MA_info_Perc = df_overview.iloc[-1]['No_200_MA_info_Perc']



# Conditional rendering based on the selected page

if page == "Trends":
    show_Trends(df_trends_display)
elif page == "Market Caps":
    show_Market_Caps()
elif page == 'Market Overview':
    market_overview()
