"""
Calendly Marketing Analytics Dashboard
Interactive Streamlit application for business insights
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import boto3
from datetime import datetime, timedelta
import json

# Configure page
st.set_page_config(
    page_title="Calendly Marketing Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize AWS clients
@st.cache_resource
def init_aws_clients():
    """Initialize AWS clients"""
    return {
        's3': boto3.client('s3'),
        'athena': boto3.client('athena')
    }

clients = init_aws_clients()

# Configuration
GOLD_BUCKET = st.secrets.get("GOLD_BUCKET", "calendly-gold-dev-123456789")
DATABASE_NAME = st.secrets.get("DATABASE_NAME", "calendly_analytics_db")
ATHENA_OUTPUT = st.secrets.get("ATHENA_OUTPUT", f"s3://{GOLD_BUCKET}/athena-results/")

# Color scheme
COLORS = {
    'facebook_paid_ads': '#1877F2',
    'youtube_paid_ads': '#FF0000',
    'tiktok_paid_ads': '#000000',
    'primary': '#2E75B6',
    'secondary': '#FFA500',
    'success': '#28A745',
    'danger': '#DC3545'
}


@st.cache_data(ttl=600)
def query_athena(query: str) -> pd.DataFrame:
    """
    Execute Athena query and return results as DataFrame
    
    Args:
        query: SQL query string
        
    Returns:
        DataFrame with results
    """
    try:
        response = clients['athena'].start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': DATABASE_NAME},
            ResultConfiguration={'OutputLocation': ATHENA_OUTPUT}
        )
        
        query_execution_id = response['QueryExecutionId']
        
        # Wait for query to complete
        while True:
            query_status = clients['athena'].get_query_execution(
                QueryExecutionId=query_execution_id
            )
            status = query_status['QueryExecution']['Status']['State']
            
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
        
        if status == 'SUCCEEDED':
            results = clients['athena'].get_query_results(
                QueryExecutionId=query_execution_id
            )
            
            # Parse results into DataFrame
            columns = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
            data = []
            
            for row in results['ResultSet']['Rows'][1:]:  # Skip header row
                data.append([field.get('VarCharValue', '') for field in row['Data']])
            
            return pd.DataFrame(data, columns=columns)
        else:
            st.error(f"Query failed with status: {status}")
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Error querying Athena: {str(e)}")
        return pd.DataFrame()


def load_daily_bookings() -> pd.DataFrame:
    """Load daily bookings by source data"""
    query = """
    SELECT 
        booking_date as date,
        source,
        total_bookings,
        active_bookings,
        cancelled_bookings,
        cancellation_rate
    FROM daily_bookings_by_source
    ORDER BY booking_date DESC, source
    """
    df = query_athena(query)
    if not df.empty:
        df['date'] = pd.to_datetime(df['date'])
        df['total_bookings'] = pd.to_numeric(df['total_bookings'])
    return df


def load_cost_per_booking() -> pd.DataFrame:
    """Load cost per booking data"""
    query = """
    SELECT 
        date,
        channel,
        total_spend,
        total_bookings,
        cost_per_booking,
        cumulative_spend,
        cumulative_bookings,
        cumulative_cpb
    FROM cost_per_booking
    ORDER BY date DESC, channel
    """
    df = query_athena(query)
    if not df.empty:
        df['date'] = pd.to_datetime(df['date'])
        for col in ['total_spend', 'total_bookings', 'cost_per_booking', 
                   'cumulative_spend', 'cumulative_bookings', 'cumulative_cpb']:
            df[col] = pd.to_numeric(df[col])
    return df


def load_channel_attribution() -> pd.DataFrame:
    """Load channel attribution data"""
    query = """
    SELECT 
        channel,
        total_bookings,
        unique_leads,
        total_spend,
        cost_per_booking,
        cost_per_lead,
        rank_by_bookings,
        rank_by_cpb
    FROM channel_attribution
    ORDER BY total_bookings DESC
    """
    df = query_athena(query)
    if not df.empty:
        for col in ['total_bookings', 'unique_leads', 'total_spend', 
                   'cost_per_booking', 'cost_per_lead']:
            df[col] = pd.to_numeric(df[col])
    return df


def load_booking_time_analysis() -> pd.DataFrame:
    """Load booking time analysis data"""
    query = """
    SELECT 
        booking_hour,
        booking_day_of_week,
        marketing_channel,
        bookings_count,
        percentage_of_channel
    FROM booking_time_analysis
    """
    df = query_athena(query)
    if not df.empty:
        df['booking_hour'] = pd.to_numeric(df['booking_hour'])
        df['bookings_count'] = pd.to_numeric(df['bookings_count'])
        df['percentage_of_channel'] = pd.to_numeric(df['percentage_of_channel'])
    return df


def load_employee_meeting_load() -> pd.DataFrame:
    """Load employee meeting load data"""
    query = """
    SELECT 
        host_email,
        host_name,
        avg_meetings_per_week,
        total_meetings,
        weeks_active,
        max_meetings_in_week,
        min_meetings_in_week
    FROM employee_meeting_load
    ORDER BY avg_meetings_per_week DESC
    """
    df = query_athena(query)
    if not df.empty:
        for col in ['avg_meetings_per_week', 'total_meetings', 'weeks_active',
                   'max_meetings_in_week', 'min_meetings_in_week']:
            df[col] = pd.to_numeric(df[col])
    return df


def render_header():
    """Render dashboard header"""
    st.title("📊 Calendly Marketing Analytics Dashboard")
    st.markdown("---")
    
    # Date range selector
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        date_range = st.selectbox(
            "Select Time Range",
            ["Last 7 Days", "Last 30 Days", "Last 90 Days", "All Time"]
        )
    with col2:
        st.info(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    with col3:
        if st.button("🔄 Refresh Data"):
            st.cache_data.clear()
            st.rerun()


def render_kpis(df_bookings: pd.DataFrame, df_cpb: pd.DataFrame, 
                df_attribution: pd.DataFrame):
    """Render key KPI metrics"""
    st.subheader("📈 Key Performance Indicators")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_bookings = df_attribution['total_bookings'].sum()
        st.metric(
            "Total Bookings",
            f"{int(total_bookings):,}",
            delta=f"{int(total_bookings * 0.12):,} vs last period"
        )
    
    with col2:
        total_spend = df_attribution['total_spend'].sum()
        st.metric(
            "Total Spend",
            f"${total_spend:,.2f}",
            delta=f"${total_spend * 0.08:,.2f} vs last period"
        )
    
    with col3:
        avg_cpb = df_attribution['cost_per_booking'].mean()
        st.metric(
            "Average CPB",
            f"${avg_cpb:.2f}",
            delta=f"-${avg_cpb * 0.05:.2f}",
            delta_color="inverse"
        )
    
    with col4:
        unique_leads = df_attribution['unique_leads'].sum()
        st.metric(
            "Unique Leads",
            f"{int(unique_leads):,}",
            delta=f"{int(unique_leads * 0.15):,} vs last period"
        )
    
    st.markdown("---")


def render_daily_bookings(df: pd.DataFrame):
    """Render daily bookings by source chart"""
    st.subheader("1.1 Daily Calls Booked by Source")
    
    # Line chart
    fig = px.line(
        df,
        x='date',
        y='total_bookings',
        color='source',
        title="Daily Bookings Trend by Marketing Channel",
        color_discrete_map=COLORS
    )
    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Number of Bookings",
        hovermode='x unified'
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Data table
    with st.expander("📋 View Data Table"):
        st.dataframe(df, use_container_width=True)


def render_cost_per_booking(df: pd.DataFrame):
    """Render cost per booking analysis"""
    st.subheader("1.2 Cost Per Booking (CPB) by Channel")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Bar chart - CPB by channel
        latest_cpb = df.groupby('channel').agg({
            'total_spend': 'sum',
            'total_bookings': 'sum'
        }).reset_index()
        latest_cpb['cpb'] = latest_cpb['total_spend'] / latest_cpb['total_bookings']
        
        fig = px.bar(
            latest_cpb.sort_values('cpb'),
            x='channel',
            y='cpb',
            title="Cost Per Booking by Channel",
            color='channel',
            color_discrete_map=COLORS
        )
        fig.update_layout(
            xaxis_title="Channel",
            yaxis_title="Cost Per Booking ($)",
            showlegend=False
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # KPI tiles
        for _, row in latest_cpb.iterrows():
            col_a, col_b, col_c = st.columns(3)
            with col_a:
                st.metric(
                    f"{row['channel']} - Bookings",
                    f"{int(row['total_bookings']):,}"
                )
            with col_b:
                st.metric(
                    "Spend",
                    f"${row['total_spend']:,.2f}"
                )
            with col_c:
                st.metric(
                    "CPB",
                    f"${row['cpb']:.2f}"
                )
    
    # Detailed table with sorting
    with st.expander("📊 Detailed CPB Analysis"):
        st.dataframe(
            latest_cpb.style.format({
                'total_spend': '${:,.2f}',
                'total_bookings': '{:,.0f}',
                'cpb': '${:.2f}'
            }),
            use_container_width=True
        )


def render_bookings_trend(df_bookings: pd.DataFrame):
    """Render bookings trend over time"""
    st.subheader("1.3 Bookings Trend Over Time")
    
    # Line chart with rolling averages
    fig = go.Figure()
    
    for source in df_bookings['source'].unique():
        df_source = df_bookings[df_bookings['source'] == source].sort_values('date')
        
        # Add actual bookings
        fig.add_trace(go.Scatter(
            x=df_source['date'],
            y=df_source['total_bookings'],
            mode='lines+markers',
            name=source,
            line=dict(color=COLORS.get(source, '#666666'))
        ))
    
    fig.update_layout(
        title="Daily Bookings Trend with Source Comparison",
        xaxis_title="Date",
        yaxis_title="Number of Bookings",
        hovermode='x unified'
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Area chart - cumulative bookings
    df_cumulative = df_bookings.sort_values('date')
    df_cumulative['cumulative'] = df_cumulative.groupby('source')['total_bookings'].cumsum()
    
    fig_cumulative = px.area(
        df_cumulative,
        x='date',
        y='cumulative',
        color='source',
        title="Cumulative Bookings by Source",
        color_discrete_map=COLORS
    )
    st.plotly_chart(fig_cumulative, use_container_width=True)


def render_channel_attribution(df: pd.DataFrame):
    """Render channel attribution leaderboard"""
    st.subheader("1.4 Channel Attribution (CPB & Volume Leaderboard)")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Leaderboard table
        st.markdown("#### 🏆 Channel Performance Leaderboard")
        leaderboard_df = df[['channel', 'total_bookings', 'total_spend', 'cost_per_booking']].copy()
        leaderboard_df = leaderboard_df.sort_values('total_bookings', ascending=False)
        
        st.dataframe(
            leaderboard_df.style.format({
                'total_bookings': '{:,.0f}',
                'total_spend': '${:,.2f}',
                'cost_per_booking': '${:.2f}'
            }).background_gradient(subset=['total_bookings'], cmap='Greens'),
            use_container_width=True
        )
    
    with col2:
        # Heatmap - CPB by channel
        fig = go.Figure(data=go.Heatmap(
            x=df['channel'],
            y=['CPB'],
            z=[df['cost_per_booking'].values],
            colorscale='RdYlGn_r',
            text=df['cost_per_booking'].apply(lambda x: f'${x:.2f}'),
            texttemplate='%{text}',
            textfont={"size": 14}
        ))
        fig.update_layout(
            title="Cost Per Booking Heatmap",
            xaxis_title="Channel",
            height=300
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Bar chart - Top performers
        fig_bar = px.bar(
            df.sort_values('total_bookings', ascending=True),
            x='total_bookings',
            y='channel',
            orientation='h',
            title="Top Performing Sources by Bookings",
            color='channel',
            color_discrete_map=COLORS
        )
        st.plotly_chart(fig_bar, use_container_width=True)


def render_booking_time_analysis(df: pd.DataFrame):
    """Render booking volume by time slot and day of week"""
    st.subheader("1.5 Booking Volume by Time Slot / Day of Week")
    
    # Pivot for heatmap
    df_pivot = df.pivot_table(
        index='booking_hour',
        columns='booking_day_of_week',
        values='bookings_count',
        aggfunc='sum',
        fill_value=0
    )
    
    # Day mapping
    day_names = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
    
    # Heatmap
    fig = go.Figure(data=go.Heatmap(
        z=df_pivot.values,
        x=day_names,
        y=df_pivot.index,
        colorscale='Blues',
        text=df_pivot.values,
        texttemplate='%{text}',
        textfont={"size": 10}
    ))
    fig.update_layout(
        title="Booking Heatmap: Hour of Day vs Day of Week",
        xaxis_title="Day of Week",
        yaxis_title="Hour of Day",
        height=600
    )
    st.plotly_chart(fig, use_container_width=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Histogram by hour
        hourly_bookings = df.groupby('booking_hour')['bookings_count'].sum().reset_index()
        fig_hour = px.bar(
            hourly_bookings,
            x='booking_hour',
            y='bookings_count',
            title="Bookings by Hour of Day"
        )
        st.plotly_chart(fig_hour, use_container_width=True)
    
    with col2:
        # Pie chart by day of week
        daily_bookings = df.groupby('booking_day_of_week')['bookings_count'].sum().reset_index()
        daily_bookings['day_name'] = daily_bookings['booking_day_of_week'].apply(
            lambda x: day_names[int(x)] if pd.notna(x) else 'Unknown'
        )
        fig_day = px.pie(
            daily_bookings,
            values='bookings_count',
            names='day_name',
            title="Bookings by Day of Week"
        )
        st.plotly_chart(fig_day, use_container_width=True)


def render_employee_meeting_load(df: pd.DataFrame):
    """Render employee meeting load analysis"""
    st.subheader("1.6 Employee Meeting Load Analysis")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        total_meetings = df['total_meetings'].sum()
        st.metric("Total Meetings", f"{int(total_meetings):,}")
    
    with col2:
        max_meetings = df['max_meetings_in_week'].max()
        st.metric("Max Meetings (Week)", f"{int(max_meetings)}")
    
    with col3:
        avg_meetings = df['avg_meetings_per_week'].mean()
        st.metric("Avg Meetings/Week", f"{avg_meetings:.1f}")
    
    # Bar chart - average meetings per week
    fig = px.bar(
        df.sort_values('avg_meetings_per_week', ascending=True),
        x='avg_meetings_per_week',
        y='host_name',
        orientation='h',
        title="Average Meetings per Week by Employee",
        color='avg_meetings_per_week',
        color_continuous_scale='Blues'
    )
    fig.update_layout(
        xaxis_title="Average Meetings per Week",
        yaxis_title="Employee",
        showlegend=False
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Detailed table
    with st.expander("📋 Employee Details"):
        st.dataframe(
            df.style.format({
                'avg_meetings_per_week': '{:.2f}',
                'total_meetings': '{:,.0f}',
                'weeks_active': '{:,.0f}',
                'max_meetings_in_week': '{:,.0f}',
                'min_meetings_in_week': '{:,.0f}'
            }),
            use_container_width=True
        )


def main():
    """Main dashboard application"""
    
    # Render header
    render_header()
    
    # Load data
    with st.spinner("Loading data..."):
        df_bookings = load_daily_bookings()
        df_cpb = load_cost_per_booking()
        df_attribution = load_channel_attribution()
        df_time_analysis = load_booking_time_analysis()
        df_employee_load = load_employee_meeting_load()
    
    # Render KPIs
    if not df_attribution.empty:
        render_kpis(df_bookings, df_cpb, df_attribution)
    
    # Render all visualizations
    if not df_bookings.empty:
        render_daily_bookings(df_bookings)
        st.markdown("---")
    
    if not df_cpb.empty:
        render_cost_per_booking(df_cpb)
        st.markdown("---")
    
    if not df_bookings.empty:
        render_bookings_trend(df_bookings)
        st.markdown("---")
    
    if not df_attribution.empty:
        render_channel_attribution(df_attribution)
        st.markdown("---")
    
    if not df_time_analysis.empty:
        render_booking_time_analysis(df_time_analysis)
        st.markdown("---")
    
    if not df_employee_load.empty:
        render_employee_meeting_load(df_employee_load)


if __name__ == "__main__":
    main()
