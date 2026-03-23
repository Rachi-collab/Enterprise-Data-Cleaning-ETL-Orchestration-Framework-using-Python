import streamlit as st
import pandas as pd
import psycopg2

# -------------------------------
# PAGE CONFIG
# -------------------------------
st.set_page_config(page_title="Retail Dashboard", layout="wide")

# -------------------------------
# DATABASE CONNECTION
# -------------------------------
@st.cache_data
def get_data():
    conn = psycopg2.connect(
        host="localhost",
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432
    )

    query = "SELECT * FROM retail_data"
    df = pd.read_sql(query, conn)
    df = pd.read_csv("data/processed/cleaned_retail.csv")
    df["invoicedate"] = pd.to_datetime(df["invoicedate"])
    conn.close()
    return df

df = get_data()

# -------------------------------
# DATA PREPARATION
# -------------------------------
df["invoicedate"] = pd.to_datetime(df["invoicedate"])

# -------------------------------
# TITLE
# -------------------------------
st.title("📊 Retail Data Dashboard")

# -------------------------------
# SIDEBAR FILTERS
# -------------------------------
st.sidebar.header("Filters")

start_date = st.sidebar.date_input("Start Date", df["invoicedate"].min())
end_date = st.sidebar.date_input("End Date", df["invoicedate"].max())

filtered_df = df[
    (df["invoicedate"] >= pd.to_datetime(start_date)) &
    (df["invoicedate"] <= pd.to_datetime(end_date))
]

country_list = st.sidebar.multiselect(
    "Select Country",
    options=filtered_df["country"].unique(),
    default=filtered_df["country"].unique()
)

filtered_df = filtered_df[filtered_df["country"].isin(country_list)]

# -------------------------------
# METRICS
# -------------------------------
total_revenue = filtered_df["totalamount"].sum()
total_orders = filtered_df["invoice"].nunique()

col1, col2 = st.columns(2)

with col1:
    st.metric("Total Revenue", f"${total_revenue:,.2f}")

with col2:
    st.metric("Total Orders", total_orders)

# -------------------------------
# SALES TREND
# -------------------------------
st.subheader("📈 Sales Trend Over Time")

sales_trend = (
    filtered_df.groupby(filtered_df["invoicedate"].dt.date)["totalamount"]
    .sum()
)

st.line_chart(sales_trend)

# -------------------------------
# TOP COUNTRIES
# -------------------------------
st.subheader("🌍 Top Countries by Revenue")

country_data = (
    filtered_df.groupby("country")["totalamount"]
    .sum()
    .sort_values(ascending=False)
    .head(10)
)

st.bar_chart(country_data)

# -------------------------------
# TOP PRODUCTS
# -------------------------------
st.subheader("🛍 Top Products")

product_data = (
    filtered_df.groupby("description")["totalamount"]
    .sum()
    .sort_values(ascending=False)
    .head(10)
)

st.bar_chart(product_data)

# -------------------------------
# DATA TABLE
# -------------------------------
st.subheader("📄 Filtered Data Preview")
st.dataframe(filtered_df.head(50))