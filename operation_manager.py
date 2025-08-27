import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import psycopg2
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import json
import os
import streamlit as st

class OperationsDashboard:
    def __init__(self, db_config=None):
        self.db_config = db_config
        self.data = {}
        self.kpi_results = {}
        
        # Create output directory if it doesn't exist
        os.makedirs('output', exist_ok=True)
        
        if db_config:
            self.engine = self.init_connection()
        else:
            st.info("Running in demo mode with sample data")
            self.generate_sample_data()
    
    def init_connection(self):
        """Initialize database connection"""
        connection_string = f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
        return create_engine(connection_string)
    
    def generate_sample_data(self):
        """Generate sample data for testing without database"""
        st.info("Generating sample data...")
        
        # Sample bookings data
        self.data['bookings'] = pd.DataFrame({
            'id': range(1, 101),
            'booking_status': np.random.choice(['completed', 'cancelled', 'pending'], 100, p=[0.8, 0.1, 0.1]),
            'created_at': [datetime.now() - timedelta(days=np.random.randint(1, 30)) for _ in range(100)],
            'trip': [f"TRIP{str(i).zfill(3)}" for i in range(1, 101)],
            'route_id': np.random.randint(1, 6, 100)
        })
        
        # Sample trip timings
        self.data['trip_timings'] = pd.DataFrame({
            'trip_id': [f"TRIP{str(i).zfill(3)}" for i in range(1, 101)],
            'start': [datetime.now().replace(hour=8, minute=0) + timedelta(minutes=np.random.randint(-15, 30)) for _ in range(100)],
            'end': [datetime.now().replace(hour=12, minute=0) + timedelta(minutes=np.random.randint(-15, 30)) for _ in range(100)],
            'trip_time': [datetime.now().replace(hour=8, minute=0) for _ in range(100)]
        })
        
        # Sample trip durations
        self.data['trip_durations'] = pd.DataFrame({
            'trip_id': [f"TRIP{str(i).zfill(3)}" for i in range(1, 101)],
            'expected_duration': np.random.randint(180, 300, 100),  # 3-5 hours
            'real_duration': np.random.randint(170, 310, 100),
            'status': np.random.choice(['on_time', 'delayed'], 100, p=[0.8, 0.2])
        })
        
        # Sample vehicles data
        self.data['vehicles'] = pd.DataFrame({
            'id': range(1, 21),
            'registration_number': [f"VEH{str(i).zfill(3)}" for i in range(1, 21)],
            'status': [True] * 15 + [False] * 5,  # 15 operational, 5 not
            'brand_name': np.random.choice(['Mercedes', 'Volvo', 'Scania', 'MAN'], 20)
        })
        
        # Sample corrective maintenances
        maintenance_dates = [datetime.now() - timedelta(days=np.random.randint(1, 90)) for _ in range(30)]
        maintenance_dates.sort()
        
        self.data['corrective_maintenances'] = pd.DataFrame({
            'id': range(1, 31),
            'date': maintenance_dates,
            'vehicle_id': np.random.randint(1, 21, 30),
            'duration': np.random.randint(2, 48, 30),  # 2-48 hours
            'name': [f"Maintenance {i}" for i in range(1, 31)]
        })
        
        # Sample users (drivers)
        self.data['users'] = pd.DataFrame({
            'id': range(1, 11),
            'firstname': [f"Driver{chr(65+i)}" for i in range(10)],
            'lastname': [f"Last{chr(65+i)}" for i in range(10)],
            'position': ['Driver'] * 10
        })
        
        # Sample attendances
        self.data['attendances'] = pd.DataFrame({
            'id': range(1, 51),
            'user_id': np.random.randint(1, 11, 50),
            'date': [datetime.now().date() - timedelta(days=i) for i in range(50)],
            'presence_type': np.random.choice(['present', 'absent', 'sick'], 50, p=[0.8, 0.1, 0.1])
        })
        
        # Sample routes
        self.data['routes'] = pd.DataFrame({
            'id': range(1, 6),
            'name': [f"Route {chr(65+i)}" for i in range(5)],
            'number': [f"R{100+i}" for i in range(5)]
        })
        
        st.success("Sample data generated successfully!")
    
    def load_data_from_db(self):
        """Load data from database"""
        if not self.engine:
            st.error("No database connection configured")
            return
        
        st.info("Loading data from database...")
        
        # Load all necessary tables
        table_names = [
            'bookings', 'vehicle_schedules', 'trip_timings', 'trip_durations',
            'vehicles', 'corrective_maintenances', 'users', 'routes',
            'booking_operations', 'maintenance_tasks', 'attendances', 'shifts'
        ]
        
        for table in table_names:
            try:
                self.data[table] = pd.read_sql_table(table, self.engine)
                st.success(f"Loaded {table} ({len(self.data[table])} rows)")
            except Exception as e:
                st.warning(f"Could not load {table}: {str(e)}")
                self.data[table] = pd.DataFrame()  # Empty dataframe as fallback
        
        st.success("Data loading completed!")
    
    def calculate_kpis(self):
        """Calculate all KPIs"""
        st.info("Calculating KPIs...")
        
        # On-Time Performance
        departure_otp, arrival_otp = self.calculate_otp()
        self.kpi_results['departure_otp'] = departure_otp
        self.kpi_results['arrival_otp'] = arrival_otp
        
        # Trip Completion
        completion_rate, cancellation_rate = self.calculate_trip_completion()
        self.kpi_results['completion_rate'] = completion_rate
        self.kpi_results['cancellation_rate'] = cancellation_rate
        
        # MTBF
        mtbf = self.calculate_mtbf()
        self.kpi_results['mtbf'] = mtbf
        
        # Fleet Downtime
        downtime_pct = self.calculate_fleet_downtime()
        self.kpi_results['downtime_pct'] = downtime_pct
        
        # Staff Readiness
        staff_readiness = self.calculate_staff_readiness()
        self.kpi_results['staff_readiness'] = staff_readiness
        
        # Operational Vehicles
        operational_vehicles = self.calculate_operational_vehicles()
        self.kpi_results['operational_vehicles'] = operational_vehicles
        self.kpi_results['total_vehicles'] = len(self.data['vehicles']) if not self.data['vehicles'].empty else 0
        
        st.success("KPI calculation completed!")
    
    def calculate_otp(self):
        """Calculate On-Time Performance"""
        if self.data['trip_timings'].empty or self.data['trip_durations'].empty:
            return 0, 0
        
        try:
            # Merge trip timings with durations
            merged = pd.merge(self.data['trip_timings'], self.data['trip_durations'], on='trip_id', how='left', suffixes=('_timing', '_duration'))
            
            # Calculate departure and arrival punctuality
            if 'start' in merged.columns and 'trip_time_timing' in merged.columns:
                merged['departed_on_time'] = merged['start'] <= (merged['trip_time_timing'] + pd.Timedelta(minutes=5))
                departure_otp = (merged['departed_on_time'].sum() / len(merged)) * 100
            else:
                departure_otp = 0
                
            if 'end' in merged.columns and 'expected_duration' in merged.columns:
                merged['arrived_on_time'] = merged['end'] <= (merged['trip_time_timing'] + pd.to_timedelta(merged['expected_duration'], unit='m') + pd.Timedelta(minutes=10))
                arrival_otp = (merged['arrived_on_time'].sum() / len(merged)) * 100
            else:
                arrival_otp = 0
                
            return departure_otp, arrival_otp
        except Exception as e:
            st.error(f"Error calculating OTP: {str(e)}")
            return 0, 0

    def calculate_trip_completion(self):
        """Calculate trip completion and cancellation rates"""
        if self.data['bookings'].empty:
            return 0, 0
        
        try:
            bookings = self.data['bookings']
            total_trips = len(bookings)
            
            if 'booking_status' in bookings.columns:
                completed = bookings[bookings['booking_status'] == 'completed'].shape[0]
                cancelled = bookings[bookings['booking_status'] == 'cancelled'].shape[0]
            else:
                completed = 0
                cancelled = 0
            
            completion_rate = (completed / total_trips) * 100 if total_trips > 0 else 0
            cancellation_rate = (cancelled / total_trips) * 100 if total_trips > 0 else 0
            
            return completion_rate, cancellation_rate
        except Exception as e:
            st.error(f"Error calculating trip completion: {str(e)}")
            return 0, 0

    def calculate_mtbf(self):
        """Calculate Mean Time Between Failures"""
        if self.data['corrective_maintenances'].empty:
            return 0
        
        try:
            maint = self.data['corrective_maintenances']
            if len(maint) < 2:
                return 0
            
            # Ensure we have date column
            if 'date' not in maint.columns:
                return 0
                
            # Sort by date and calculate time between failures
            maint = maint.sort_values('date')
            maint['date'] = pd.to_datetime(maint['date'])
            time_diffs = maint['date'].diff().dt.total_seconds() / 3600  # Convert to hours
            
            # Calculate average (ignore first NaN value)
            return time_diffs.mean() if len(time_diffs) > 1 else 0
        except Exception as e:
            st.error(f"Error calculating MTBF: {str(e)}")
            return 0

    def calculate_fleet_downtime(self):
        """Calculate fleet downtime percentage"""
        if self.data['vehicles'].empty or self.data['corrective_maintenances'].empty:
            return 0
        
        try:
            vehicles = self.data['vehicles']
            corrective_maint = self.data['corrective_maintenances']
            
            # Calculate total available time (assuming 30 days for all vehicles)
            total_fleet_hours = len(vehicles) * 24 * 30
            
            # Calculate downtime from maintenance records
            if 'duration' in corrective_maint.columns:
                downtime_hours = corrective_maint['duration'].sum()
            else:
                downtime_hours = 0
            
            # Calculate downtime percentage
            downtime_pct = (downtime_hours / total_fleet_hours) * 100 if total_fleet_hours > 0 else 0
            
            return downtime_pct
        except Exception as e:
            st.error(f"Error calculating fleet downtime: {str(e)}")
            return 0

    def calculate_staff_readiness(self):
        """Calculate staff readiness score"""
        if self.data['attendances'].empty:
            return 0
        
        try:
            attendances = self.data['attendances']
            
            # Calculate percentage of staff present
            if 'presence_type' in attendances.columns:
                present_count = attendances[attendances['presence_type'] == 'present'].shape[0]
            else:
                present_count = 0
                
            total_count = len(attendances)
            
            return (present_count / total_count) * 100 if total_count > 0 else 0
        except Exception as e:
            st.error(f"Error calculating staff readiness: {str(e)}")
            return 0

    def calculate_operational_vehicles(self):
        """Calculate number of operational vehicles"""
        if self.data['vehicles'].empty:
            return 0
        
        try:
            if 'status' in self.data['vehicles'].columns:
                return self.data['vehicles']['status'].sum()
            else:
                return len(self.data['vehicles'])
        except Exception as e:
            st.error(f"Error calculating operational vehicles: {str(e)}")
            return 0

    def create_visualizations(self):
        """Create all visualizations"""
        st.info("Creating visualizations...")
        
        # 1. OTP Gauge Charts
        st.subheader("On-Time Performance")
        fig_otp = self.create_otp_chart()
        st.plotly_chart(fig_otp, use_container_width=True)
        
        # 2. Trip Breakdown Chart
        st.subheader("Trip Completion Breakdown")
        fig_trip = self.create_trip_breakdown_chart()
        st.plotly_chart(fig_trip, use_container_width=True)
        
        # 3. Delay Root Cause Chart
        st.subheader("Delay Root Causes")
        fig_delay = self.create_delay_root_cause_chart()
        st.plotly_chart(fig_delay, use_container_width=True)
        
        # 4. Vehicle Reliability Chart
        st.subheader("Vehicle Reliability")
        fig_vehicle = self.create_vehicle_reliability_chart()
        st.plotly_chart(fig_vehicle, use_container_width=True)
        
        # 5. Driver Performance Chart
        st.subheader("Driver Performance")
        fig_driver = self.create_driver_performance_chart()
        st.plotly_chart(fig_driver, use_container_width=True)
        
        st.success("All visualizations created!")

    def create_otp_chart(self):
        """Create OTP gauge charts"""
        fig = go.Figure()
        
        fig.add_trace(go.Indicator(
            mode = "gauge+number",
            value = self.kpi_results['departure_otp'],
            number = {'suffix': '%'},
            title = {'text': "On-Time Departure %"},
            domain = {'x': [0, 0.5], 'y': [0, 1]},
            gauge = {
                'axis': {'range': [0, 100]},
                'bar': {'color': "darkblue"},
                'steps': [
                    {'range': [0, 85], 'color': "lightgray"},
                    {'range': [85, 95], 'color': "gray"},
                    {'range': [95, 100], 'color': "lightgreen"}
                ]
            }
        ))
        
        fig.add_trace(go.Indicator(
            mode = "gauge+number",
            value = self.kpi_results['arrival_otp'],
            number = {'suffix': '%'},
            title = {'text': "On-Time Arrival %"},
            domain = {'x': [0.5, 1], 'y': [0, 1]},
            gauge = {
                'axis': {'range': [0, 100]},
                'bar': {'color': "darkblue"},
                'steps': [
                    {'range': [0, 85], 'color': "lightgray"},
                    {'range': [85, 95], 'color': "gray"},
                    {'range': [95, 100], 'color': "lightgreen"}
                ]
            }
        ))
        
        fig.update_layout(height=300, margin=dict(l=50, r=50, b=50, t=50))
        return fig

    def create_trip_breakdown_chart(self):
        """Create trip breakdown pie chart"""
        labels = ['Completed', 'Cancelled', 'Other']
        values = [
            self.kpi_results['completion_rate'], 
            self.kpi_results['cancellation_rate'], 
            max(0, 100 - self.kpi_results['completion_rate'] - self.kpi_results['cancellation_rate'])
        ]
        
        fig = px.pie(values=values, names=labels, title='Trip Completion Breakdown')
        fig.update_traces(textinfo='percent+label')
        return fig

    def create_delay_root_cause_chart(self):
        """Create delay root cause pie chart"""
        # Using placeholder data
        reasons = ['Traffic', 'Mechanical Issues', 'Weather', 'Staff Availability', 'Other']
        counts = [35, 25, 20, 15, 5]
        
        fig = px.pie(values=counts, names=reasons, title='Delay Root Causes')
        fig.update_traces(textinfo='percent+label')
        return fig

    def create_vehicle_reliability_chart(self):
        """Create vehicle reliability scorecard"""
        if self.data['vehicles'].empty or self.data['corrective_maintenances'].empty:
            return px.bar(title='No vehicle data available')
        
        try:
            vehicles = self.data['vehicles']
            corrective_maint = self.data['corrective_maintenances']
            
            # Count maintenance events per vehicle
            maint_count = corrective_maint['vehicle_id'].value_counts().reset_index()
            maint_count.columns = ['vehicle_id', 'maintenance_count']
            
            # Merge with vehicle data
            vehicle_reliability = pd.merge(vehicles, maint_count, on='vehicle_id', how='left')
            vehicle_reliability['maintenance_count'] = vehicle_reliability['maintenance_count'].fillna(0)
            
            # Create reliability score (lower maintenance count = higher reliability)
            max_count = vehicle_reliability['maintenance_count'].max() if len(vehicle_reliability) > 0 else 1
            vehicle_reliability['reliability_score'] = 100 - (vehicle_reliability['maintenance_count'] / max_count * 100)
            
            # Get top 10 vehicles by reliability
            top_vehicles = vehicle_reliability.nlargest(10, 'reliability_score')
            
            # Create chart
            fig = px.bar(
                top_vehicles,
                x='reliability_score', 
                y='registration_number',
                orientation='h',
                title='Top 10 Vehicles by Reliability Score'
            )
            fig.update_layout(yaxis_title='Vehicle', xaxis_title='Reliability Score')
            return fig
        except Exception as e:
            st.error(f"Error creating vehicle reliability chart: {str(e)}")
            return px.bar(title='Error loading vehicle data')

    def create_driver_performance_chart(self):
        """Create driver performance radar chart"""
        # Using placeholder data
        drivers = ['Driver A', 'Driver B', 'Driver C', 'Driver D']
        categories = ['Punctuality', 'Safety', 'Fuel Efficiency', 'Customer Satisfaction', 'Route Knowledge']
        
        fig = go.Figure()
        
        for driver in drivers:
            fig.add_trace(go.Scatterpolar(
                r=[np.random.randint(70, 100) for _ in range(5)],
                theta=categories,
                fill='toself',
                name=driver
            ))
        
        fig.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
            title='Driver Performance by Category'
        )
        
        return fig

    def generate_report(self):
        """Generate a comprehensive report"""
        st.header("OPERATIONS MANAGER DASHBOARD REPORT")
        
        st.subheader("Key Performance Indicators:")
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("On-Time Departure", f"{self.kpi_results['departure_otp']:.1f}%")
            st.metric("On-Time Arrival", f"{self.kpi_results['arrival_otp']:.1f}%")
            st.metric("Trip Completion Rate", f"{self.kpi_results['completion_rate']:.1f}%")
            st.metric("Cancellation Rate", f"{self.kpi_results['cancellation_rate']:.1f}%")
            
        with col2:
            st.metric("Mean Time Between Failures", f"{self.kpi_results['mtbf']:.1f} hours")
            st.metric("Fleet Downtime", f"{self.kpi_results['downtime_pct']:.1f}%")
            st.metric("Staff Readiness", f"{self.kpi_results['staff_readiness']:.1f}%")
            st.metric("Operational Vehicles", f"{self.kpi_results['operational_vehicles']}/{self.kpi_results['total_vehicles']}")

# Streamlit app
def main():
    st.set_page_config(page_title="Operations Dashboard", page_icon="🚌", layout="wide")
    st.title("🚌 Operations Manager Dashboard")
    
    # Initialize dashboard with or without database connection
    if st.sidebar.checkbox("Use Database Connection"):
        # Get database credentials from Streamlit secrets or user input
        db_config = {
            'host': st.secrets.get("DB_HOST", "51.178.30.30"),
            'database': st.secrets.get("DB_NAME", "rawahel_test"),
            'user': st.secrets.get("DB_USER", "readonly_user"),
            'password': st.secrets.get("DB_PASSWORD", ""),
            'port': st.secrets.get("DB_PORT", 5432)
        }
        
        # If password is not in secrets, ask for it
        if not db_config['password']:
            db_config['password'] = st.sidebar.text_input("Database Password", type="password")
            
        dashboard = OperationsDashboard(db_config)
        
        if st.sidebar.button("Load Data from Database"):
            dashboard.load_data_from_db()
    else:
        dashboard = OperationsDashboard()
    
    # Calculate KPIs
    if st.sidebar.button("Calculate KPIs"):
        dashboard.calculate_kpis()
        dashboard.generate_report()
    
    # Create visualizations
    if st.sidebar.button("Generate Visualizations"):
        if not dashboard.kpi_results:
            st.warning("Please calculate KPIs first")
        else:
            dashboard.create_visualizations()

if __name__ == "__main__":
    main()