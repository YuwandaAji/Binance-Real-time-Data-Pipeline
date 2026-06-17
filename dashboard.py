import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import time

# === KONEKSI POSTGRESQL ===
def get_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="ecommerce",
        user="postgres",
        password="secret"
    )

def get_ticker():
    conn = get_connection()
    df = pd.read_sql("""
        SELECT DISTINCT ON (symbol) 
            symbol, price, change_pct, high_24h, low_24h, volume_24h, event_time
        FROM ticker_latest
        ORDER BY symbol, event_time DESC
    """, conn)
    conn.close()
    return df

def get_trades():
    conn = get_connection()
    df = pd.read_sql("""
        SELECT window_start, symbol, side, avg_price, total_volume
        FROM trades_summary
        ORDER BY window_start DESC
        LIMIT 100
    """, conn)
    conn.close()
    return df

# === UI ===
st.set_page_config(
    page_title="Binance Real-time Pipeline",
    page_icon="",
    layout="wide"
)

st.title("Binance Real-time Data Pipeline")
st.caption(f"Last updated: {datetime.now().strftime('%H:%M:%S')} | Auto-refresh every 5 seconds")

# Ticker cards
st.subheader(" Live Prices")
ticker_df = get_ticker()

if not ticker_df.empty:
    cols = st.columns(len(ticker_df))
    for i, row in ticker_df.iterrows():
        with cols[i]:
            color = "#00FF00" if row['change_pct'] >= 0 else "#FF0000"
            st.metric(
                label=row['symbol'],
                value=f"${row['price']:,.2f}",
                delta=f"{row['change_pct']:+.2f}%"
            )

st.divider()

# Trades chart
st.subheader("Trading Volume per Minute")
trades_df = get_trades()

if not trades_df.empty:
    col1, col2 = st.columns(2)

    with col1:
        # Volume per coin
        fig1 = px.bar(
            trades_df.groupby('symbol')['total_volume'].sum().reset_index(),
            x='symbol', y='total_volume',
            title='Total Volume per Coin',
            color='symbol'
        )
        st.plotly_chart(fig1, use_container_width=True)

    with col2:
        # BUY vs SELL
        fig2 = px.pie(
            trades_df.groupby('side')['total_volume'].sum().reset_index(),
            values='total_volume', names='side',
            title='BUY vs SELL Volume',
            color_discrete_map={'BUY': '#00CC96', 'SELL': '#EF553B'}
        )
        st.plotly_chart(fig2, use_container_width=True)

    # Harga dari waktu ke waktu
    st.subheader("Price Over Time")
    fig3 = px.line(
        trades_df,
        x='window_start', y='avg_price',
        color='symbol',
        title='Average Price per Minute'
    )
    st.plotly_chart(fig3, use_container_width=True)

# Auto refresh
time.sleep(5)
st.rerun()