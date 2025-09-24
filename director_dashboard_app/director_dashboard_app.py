import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from datetime import datetime
import psycopg2
import warnings

warnings.filterwarnings('ignore')

# ==============================
# CONFIG
# ==============================
st.set_page_config(
    page_title="Director Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==============================
# DASHBOARD CLASS
# ==============================
class DirectorDashboard:
    def __init__(self, period="30d"):
        self.connection = None
        self.connect()
        self.last_update = datetime.now()
        self.set_period(period)

    def set_period(self, period):
        self.period_label = period
        if period == "30d":
            self.interval = "30 days"
        elif period == "60d":
            self.interval = "60 days"
        elif period == "365d":
            self.interval = "365 days"
        elif period == "6m":
            self.interval = "6 months"
        else:
            self.interval = "30 days"

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                host=st.secrets["postgres"]["host"],
                port=st.secrets["postgres"]["port"],
                database=st.secrets["postgres"]["database"],
                user=st.secrets["postgres"]["user"],
                password=st.secrets["postgres"]["password"]
            )
        except Exception as e:
            st.error(f"❌ Error connecting to database: {e}")

    def execute_query(self, query):
        """Execute query and safely handle corrupted UTF-8 bytes."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                colnames = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()

                safe_data = []
                for row in data:
                    safe_row = []
                    for val in row:
                        if isinstance(val, bytes):
                            safe_row.append(val.decode("utf-8", errors="replace"))
                        else:
                            safe_row.append(val)
                    safe_data.append(safe_row)

                return pd.DataFrame(safe_data, columns=colnames)
        except Exception as e:
            st.error(f"Query error: {e}")
            return pd.DataFrame()

    def close(self):
        if self.connection:
            self.connection.close()

    # ==============================
    # FINANCIAL KPIs
    # ==============================
    def get_gross_revenue(self):
        query = f"""
        SELECT COALESCE(SUM(debit), 0) as gross_revenue 
        FROM transactions 
        WHERE type = 'revenue' 
        AND date >= CURRENT_DATE - INTERVAL '{self.interval}'
        """
        df = self.execute_query(query)
        if df.empty or 'gross_revenue' not in df.columns:
            return 0
        return df['gross_revenue'].iloc[0]

    def get_net_profit(self):
        query = f"""
        SELECT 
            COALESCE(SUM(CASE WHEN type = 'revenue' THEN debit ELSE 0 END), 0) - 
            COALESCE(SUM(CASE WHEN type = 'expense' THEN credit ELSE 0 END), 0) as net_profit
        FROM transactions
        WHERE date >= CURRENT_DATE - INTERVAL '{self.interval}'
        """
        df = self.execute_query(query)
        if df.empty or 'net_profit' not in df.columns:
            return 0
        return df['net_profit'].iloc[0]

    def get_commission_costs(self):
        query = f"""
        SELECT COALESCE(SUM(value), 0) as total_commissions
        FROM agency_commissions 
        WHERE created_at >= CURRENT_DATE - INTERVAL '{self.interval}'
        """
        df = self.execute_query(query)
        if df.empty or 'total_commissions' not in df.columns:
            return 0
        return df['total_commissions'].iloc[0]

    # ==============================
    # OPERATIONAL KPIs
    # ==============================
    def get_fleet_utilization(self):
        query = """
        SELECT 
            COUNT(DISTINCT v.id) as total_vehicles,
            COUNT(DISTINCT vs.vehicle_id) as active_vehicles,
            CASE WHEN COUNT(DISTINCT v.id) > 0 THEN
                ROUND((COUNT(DISTINCT vs.vehicle_id) * 100.0 / COUNT(DISTINCT v.id))::numeric, 2)
            ELSE 0 END as utilization_rate
        FROM vehicles v
        LEFT JOIN vehicle_schedules vs ON v.id = vs.vehicle_id 
        WHERE v.status = true
        """
        df = self.execute_query(query)
        if df.empty:
            return 0, 0, 0
        return df['utilization_rate'].iloc[0], df['active_vehicles'].iloc[0], df['total_vehicles'].iloc[0]

    def get_rofa(self):
        query = f"""
        SELECT 
            COALESCE(SUM(t.debit), 0) as total_revenue,
            COUNT(DISTINCT v.id) as total_fleet,
            CASE WHEN COUNT(DISTINCT v.id) > 0 THEN
                ROUND((COALESCE(SUM(t.debit), 0) / COUNT(DISTINCT v.id))::numeric, 2)
            ELSE 0 END as rofa
        FROM transactions t
        CROSS JOIN vehicles v
        WHERE t.type = 'revenue' 
        AND t.date >= CURRENT_DATE - INTERVAL '{self.interval}'
        AND v.status = true
        """
        df = self.execute_query(query)
        if df.empty:
            return 0, 0, 0
        return df['rofa'].iloc[0], df['total_revenue'].iloc[0], df['total_fleet'].iloc[0]

    def get_rask_simple(self):
        query = f"""
        WITH revenue_data AS (
            SELECT COALESCE(SUM(t.debit), 0) as total_revenue
            FROM transactions t
            WHERE t.type = 'revenue' 
            AND t.date >= CURRENT_DATE - INTERVAL '{self.interval}'
        ),
        booking_count AS (
            SELECT COUNT(*) as total_bookings
            FROM bookings 
            WHERE created_at >= CURRENT_DATE - INTERVAL '{self.interval}'
        ),
        avg_seats AS (
            SELECT COALESCE(AVG(total_seat), 40) as avg_seats 
            FROM fleet_types 
            WHERE total_seat > 0
        ),
        avg_distance AS (
            SELECT COALESCE(AVG(distance), 100) as avg_distance 
            FROM route_segments 
            WHERE distance > 0
        )
        SELECT 
            r.total_revenue,
            b.total_bookings * s.avg_seats * d.avg_distance as estimated_ask,
            CASE WHEN (b.total_bookings * s.avg_seats * d.avg_distance) > 0 THEN
                ROUND((r.total_revenue / (b.total_bookings * s.avg_seats * d.avg_distance))::numeric, 4)
            ELSE 0 END as rask
        FROM revenue_data r, booking_count b, avg_seats s, avg_distance d
        """
        df = self.execute_query(query)
        if df.empty or 'rask' not in df.columns:
            return 0
        return df['rask'].iloc[0]

    # ==============================
    # CUSTOMER & BOOKING KPIs
    # ==============================
    def get_customer_retention(self):
        query = f"""
        WITH customer_bookings AS (
            SELECT 
                passenger_id,
                COUNT(*) as booking_count,
                MIN(created_at) as first_booking,
                MAX(created_at) as last_booking
            FROM bookings
            WHERE created_at >= CURRENT_DATE - INTERVAL '{self.interval}'
            GROUP BY passenger_id
        )
        SELECT 
            COUNT(*) as total_customers,
            COUNT(CASE WHEN booking_count > 1 THEN 1 END) as returning_customers,
            CASE WHEN COUNT(*) > 0 THEN
                ROUND((COUNT(CASE WHEN booking_count > 1 THEN 1 END) * 100.0 / COUNT(*))::numeric, 2)
            ELSE 0 END as retention_rate
        FROM customer_bookings
        """
        return self.execute_query(query)

    def get_booking_sources(self):
        query = f"""
        SELECT 
            'All Channels' as source_type,
            COUNT(*) as booking_count,
            COALESCE(SUM(total_price), 0) as total_revenue
        FROM bookings
        WHERE created_at >= CURRENT_DATE - INTERVAL '{self.interval}'
        UNION ALL
        SELECT 
            'Web' as source_type,
            COUNT(*) as booking_count,
            COALESCE(SUM(total_price), 0) as total_revenue
        FROM bookings
        WHERE created_at >= CURRENT_DATE - INTERVAL '{self.interval}'
        AND (booking_channel LIKE '%web%' OR booking_channel LIKE '%online%' OR booking_channel IS NULL)
        UNION ALL
        SELECT 
            'POS' as source_type,
            COUNT(*) as booking_count,
            COALESCE(SUM(total_price), 0) as total_revenue
        FROM bookings
        WHERE created_at >= CURRENT_DATE - INTERVAL '{self.interval}'
        AND (booking_channel LIKE '%pos%' OR booking_channel LIKE '%counter%')
        UNION ALL
        SELECT 
            'Mobile' as source_type,
            COUNT(*) as booking_count,
            COALESCE(SUM(total_price), 0) as total_revenue
        FROM bookings
        WHERE created_at >= CURRENT_DATE - INTERVAL '{self.interval}'
        AND (booking_channel LIKE '%mobile%' OR booking_channel LIKE '%app%')
        UNION ALL
        SELECT 
            'B2B' as source_type,
            COUNT(*) as booking_count,
            COALESCE(SUM(total_price), 0) as total_revenue
        FROM bookings
        WHERE created_at >= CURRENT_DATE - INTERVAL '{self.interval}'
        AND (booking_channel LIKE '%b2b%' OR booking_channel LIKE '%corporate%')
        ORDER BY total_revenue DESC
        """
        return self.execute_query(query)

    def get_monthly_trends(self):
        query = f"""
        SELECT 
            DATE_TRUNC('month', created_at) as month,
            COUNT(id) as booking_count,
            COALESCE(SUM(total_price), 0) as revenue,
            COALESCE(SUM(
                CASE 
                    WHEN total_price > 0 THEN total_price * 0.6
                    ELSE 0 
                END
            ), 0) as estimated_cost
        FROM bookings
        WHERE created_at >= CURRENT_DATE - INTERVAL '6 months'
        GROUP BY DATE_TRUNC('month', created_at)
        ORDER BY month
        """
        return self.execute_query(query)

    def get_agency_profitability(self):
        query = f"""
        SELECT 
            a.name as agency,
            COALESCE(SUM(ar.debit), 0) as revenue,
            COALESCE(SUM(ar.credit), 0) as cost,
            COALESCE(SUM(ar.debit - ar.credit), 0) as net_profit,
            CASE WHEN COALESCE(SUM(ar.debit), 0) > 0 THEN
                ROUND(((COALESCE(SUM(ar.debit - ar.credit), 0) * 100.0 / COALESCE(SUM(ar.debit), 0))::numeric), 2)
            ELSE 0 END as profit_margin
        FROM agencies a
        LEFT JOIN agency_reports ar ON a.id = ar.agency_id
        WHERE ar.date >= CURRENT_DATE - INTERVAL '{self.interval}'
        GROUP BY a.name
        HAVING COALESCE(SUM(ar.debit), 0) > 0
        ORDER BY net_profit DESC
        LIMIT 10
        """
        return self.execute_query(query)

# ==============================
# STREAMLIT APP
# ==============================
def main():
    st.title("📊 Director Dashboard")

    # Sidebar
    with st.sidebar:
        st.header("Settings")
        period = st.selectbox(
            "Analysis Period",
            options=["30d", "60d", "365d", "6m"],
            index=0,
            help="Select the time period for analysis"
        )

        st.header("Quick Actions")
        if st.button("🔄 Refresh Data"):
            st.rerun()

        if st.button("📊 Download Reports"):
            download_reports()

    # Dashboard
    dashboard = DirectorDashboard(period)

    # KPIs
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        revenue = dashboard.get_gross_revenue()
        st.metric("Gross Revenue", f"${revenue:,.2f}")
    with col2:
        profit = dashboard.get_net_profit()
        st.metric("Net Profit", f"${profit:,.2f}", delta_color="inverse" if profit < 0 else "normal")
    with col3:
        commission = dashboard.get_commission_costs()
        st.metric("Commission Costs", f"${commission:,.2f}")
    with col4:
        utilization, active, total = dashboard.get_fleet_utilization()
        st.metric("Fleet Utilization", f"{utilization}%", f"{active}/{total} vehicles")

# ==============================
# REPORT DOWNLOADS
# ==============================
def download_reports():
    dashboard = DirectorDashboard()
    trends = dashboard.get_monthly_trends()
    agencies = dashboard.get_agency_profitability()

    csv1 = trends.to_csv(index=False)
    csv2 = agencies.to_csv(index=False)

    st.download_button("📥 Download Monthly Trends", csv1, "monthly_trends.csv", "text/csv")
    st.download_button("📥 Download Agency Performance", csv2, "agency_performance.csv", "text/csv")

# ==============================
# RUN APP
# ==============================
if __name__ == "__main__":
    main()
