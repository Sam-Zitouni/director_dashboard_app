import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import psycopg2
from psycopg2 import sql
import streamlit as st
import warnings

warnings.filterwarnings('ignore')


# ==============================
# DATABASE CONNECTION
# ==============================
class DirectorDashboard:
    def __init__(self, db_config):
        self.conn = psycopg2.connect(**db_config)

    def run_query(self, query, params=None):
        """Safely run SQL query and return DataFrame"""
        with self.conn.cursor() as cur:
            cur.execute(query, params or ())
            colnames = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
        return pd.DataFrame(rows, columns=colnames)

    # ------------------------------
    # KPI QUERIES (always numeric)
    # ------------------------------
    def get_gross_revenue(self):
        q = """SELECT COALESCE(SUM(amount), 0) AS gross_revenue FROM bookings;"""
        df = self.run_query(q)
        return float(df.iloc[0, 0])

    def get_net_profit(self):
        q = """
        SELECT COALESCE(SUM(amount - commission), 0) AS net_profit
        FROM bookings;
        """
        df = self.run_query(q)
        return float(df.iloc[0, 0])

    def get_commission_costs(self):
        q = """SELECT COALESCE(SUM(commission), 0) AS commission_costs FROM bookings;"""
        df = self.run_query(q)
        return float(df.iloc[0, 0])

    def get_fleet_utilization(self):
        q = """
        SELECT 
            COALESCE(SUM(CASE WHEN status='active' THEN 1 ELSE 0 END), 0) AS active,
            COUNT(*) AS total
        FROM fleet;
        """
        df = self.run_query(q)
        active, total = int(df.iloc[0, 0]), int(df.iloc[0, 1])
        util_rate = (active / total * 100) if total > 0 else 0
        return util_rate, active, total

    def get_rofa(self):
        q = """
        SELECT COALESCE(SUM(amount) / NULLIF(COUNT(DISTINCT vehicle_id),0), 0) AS rofa
        FROM bookings;
        """
        df = self.run_query(q)
        return float(df.iloc[0, 0])

    def get_rask(self):
        q = """
        SELECT COALESCE(SUM(amount) / NULLIF(SUM(seat_km),0), 0) AS rask
        FROM bookings;
        """
        df = self.run_query(q)
        return float(df.iloc[0, 0])

    def get_customer_retention(self):
        q = """
        SELECT 
            COALESCE(
                (COUNT(DISTINCT customer_id) FILTER (WHERE trips > 1)::float
                / NULLIF(COUNT(DISTINCT customer_id),0)) * 100, 0
            ) AS retention
        FROM (
            SELECT customer_id, COUNT(*) AS trips
            FROM bookings
            GROUP BY customer_id
        ) t;
        """
        df = self.run_query(q)
        return float(df.iloc[0, 0])


# ==============================
# STREAMLIT DASHBOARD
# ==============================
def main():
    st.set_page_config(page_title="Director Dashboard", layout="wide")

    db_config = {
        "host": "localhost",
        "dbname": "transport_db",
        "user": "postgres",
        "password": "yourpassword",
        "port": 5432
    }
    dashboard = DirectorDashboard(db_config)

    st.title("📊 Director Dashboard")

    # KPIs Row
    gross_revenue = dashboard.get_gross_revenue()
    net_profit = dashboard.get_net_profit()
    commission_costs = dashboard.get_commission_costs()
    util_rate, active, total = dashboard.get_fleet_utilization()
    rofa = dashboard.get_rofa()
    rask = dashboard.get_rask()
    retention = dashboard.get_customer_retention()

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Gross Revenue", f"${gross_revenue:,.2f}")
        st.metric("Net Profit", f"${net_profit:,.2f}")

    with col2:
        st.metric("Commission Costs", f"${commission_costs:,.2f}")
        st.metric("Fleet Utilization", f"{util_rate:.2f}%", f"{active}/{total} vehicles")

    with col3:
        st.subheader("🚗 Operational Efficiency")
        st.write("ROFA", f"${rofa:,.2f} per vehicle")
        st.write("RASK", f"${rask:,.4f} per seat km")

    with col4:
        st.subheader("Customer Retention")
        st.write(f"{retention:.2f}%")

    st.success("✅ KPIs loaded successfully!")


if __name__ == "__main__":
    main()
