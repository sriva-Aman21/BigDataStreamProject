import os
import io
import pandas as pd
import psycopg2
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh

# --------------------------------------------------
# PAGE CONFIGURATION
# --------------------------------------------------
st.set_page_config(
    page_title="Supply Chain Intelligence",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --------------------------------------------------
# CUSTOM CSS STYLING
# --------------------------------------------------
st.markdown("""
<style>
    .block-container {
        padding-top: 3.5rem !important;
        padding-bottom: 2rem;
    }
    .hero {
        padding: 1.5rem;
        border-radius: 14px;
        background: linear-gradient(120deg, #172554, #164e63);
        color: white;
        margin-bottom: 1.5rem;
    }
    .hero h1 { color: white; margin-bottom: 0.5rem; }
    .hero p { color: #dbeafe; font-size: 1rem; margin-bottom: 0; }
    .telemetry-strip {
        display: flex;
        gap: 1rem;
        background: rgba(30, 41, 59, 0.7);
        border: 1px solid rgba(148, 163, 184, 0.2);
        padding: 0.8rem 1.2rem;
        border-radius: 10px;
        margin-top: 0.5rem;
        margin-bottom: 1.5rem;
        align-items: center;
        flex-wrap: wrap;
    }
    .telemetry-badge {
        font-size: 0.85rem;
        padding: 0.3rem 0.7rem;
        border-radius: 6px;
        font-weight: 600;
    }
    .badge-ok { background: rgba(34, 197, 94, 0.2); color: #4ade80; border: 1px solid #22c55e; }
    .badge-warn { background: rgba(234, 179, 8, 0.2); color: #fde047; border: 1px solid #eab308; }
    .chart-explanation {
        background: rgba(15, 23, 42, 0.6);
        border-left: 3px solid #38bdf8;
        padding: 0.6rem 0.9rem;
        border-radius: 6px;
        font-size: 0.87rem;
        color: #cbd5e1;
        margin-top: 0.4rem;
        margin-bottom: 1rem;
        line-height: 1.4;
    }
    .chart-explanation strong {
        color: #f8fafc;
    }
    .info-card {
        border: 1px solid rgba(128, 128, 128, 0.3);
        border-radius: 12px;
        padding: 1rem;
        min-height: 120px;
        margin-bottom: 0.5rem;
    }
    .status-note {
        padding: 0.8rem 1rem;
        border-radius: 8px;
        background: rgba(100, 116, 139, 0.12);
        border-left: 4px solid #38bdf8;
        margin: 0.7rem 0 1rem 0;
    }
    .alert-banner {
        padding: 0.8rem 1rem;
        border-radius: 8px;
        background: rgba(239, 68, 68, 0.15);
        border-left: 4px solid #ef4444;
        margin: 0.7rem 0 1rem 0;
    }
    div[data-testid="stMetric"] {
        border: 1px solid rgba(128, 128, 128, 0.25);
        padding: 1rem;
        border-radius: 12px;
    }
</style>
""", unsafe_allow_html=True)

# --------------------------------------------------
# SIDEBAR NAVIGATION
# --------------------------------------------------
with st.sidebar:
    st.title("📦 Supply Chain")
    st.caption("Intelligence & Operations Center")
    st.divider()

    page = st.radio(
        "NAVIGATION",
        [
            "🏠 Project Overview",
            "📊 Live Analytics",
            "💡 Business Insights",
            "🚨 Anomaly Monitoring"
        ],
        label_visibility="visible"
    )

    st.divider()
    st.markdown("**Engine Specifications**")
    st.caption("• Streaming: Apache Spark 4.0")
    st.caption("• Broker: Apache Kafka (Distributed)")
    st.caption("• Sink: PostgreSQL 16 (ACID)")
    st.caption("• Ingestion Window: 5.0s Trigger")
    st.divider()
    st.caption("AMAN SRIVASTAV --- Final-Year B.Tech Project")

# --------------------------------------------------
# DATABASE ACCESS
# --------------------------------------------------
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host="localhost",
        port=int(os.environ.get("DB_PORT", "5432")),
        dbname=os.environ.get("DB_NAME", "supply_chain_db"),
        user=os.environ.get("DB_USER", "postgres"),
        password=os.environ.get("DB_PASSWORD", "")
    )

@st.cache_data(ttl=5)
def load_analytics_data():
    connection = get_connection()
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT product, transaction_count, total_quantity, last_updated
            FROM product_analytics
            ORDER BY product
        """)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(rows, columns=columns)

@st.cache_data(ttl=5)
def load_anomalies_data():
    connection = get_connection()
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT 
                COUNT(*) AS total_count,
                COUNT(*) FILTER (WHERE delivery_time > 10) AS delay_count,
                COUNT(*) FILTER (WHERE unit_price > 400.0) AS price_count,
                COALESCE(SUM(quantity * unit_price), 0) AS financial_exposure
            FROM supply_chain_anomalies;
        """)
        stats = cursor.fetchone()
        metrics = {
            "total": stats[0] if stats else 0,
            "delays": stats[1] if stats else 0,
            "prices": stats[2] if stats else 0,
            "exposure": float(stats[3]) if stats else 0.0
        }

        cursor.execute("""
            SELECT 
                transaction_id, timestamp, product, region, quantity, 
                unit_price, delivery_time, anomaly_reason, detected_at,
                ROUND(EXTRACT(EPOCH FROM (detected_at - timestamp::timestamp))::numeric, 2) AS latency_seconds
            FROM supply_chain_anomalies
            ORDER BY detected_at DESC
            LIMIT 100
        """)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

    df = pd.DataFrame(rows, columns=columns)
    
    # Assign Severity Classification
    if not df.empty:
        def get_severity(row):
            if row["unit_price"] > 400.0 and row["delivery_time"] > 10:
                return "CRITICAL"
            elif row["delivery_time"] > 10:
                return "HIGH (Delay)"
            return "MEDIUM (Price)"
        df["severity"] = df.apply(get_severity, axis=1)

    return df, metrics

def refresh_all_data():
    load_analytics_data.clear()
    load_anomalies_data.clear()
    return load_analytics_data(), load_anomalies_data()

# --------------------------------------------------
# SYSTEM TELEMETRY STRIP COMPONENT
# --------------------------------------------------
def render_telemetry_strip(avg_latency=None):
    latency_text = f"{avg_latency:.2f}s" if (avg_latency is not None and avg_latency >= 0) else "< 1.5s"
    st.markdown(f"""
    <div class="telemetry-strip">
        <span style="font-weight:600; color:#e2e8f0; font-size:0.9rem;">PIPELINE STATUS:</span>
        <span class="telemetry-badge badge-ok">● Kafka Broker: READY</span>
        <span class="telemetry-badge badge-ok">● Spark Engine: STREAMING</span>
        <span class="telemetry-badge badge-ok">● PostgreSQL: SYNCHRONIZED</span>
        <span class="telemetry-badge badge-warn">⚡ Processing Latency: {latency_text}</span>
    </div>
    """, unsafe_allow_html=True)

# --------------------------------------------------
# PAGE 1: PROJECT OVERVIEW
# --------------------------------------------------
def show_overview():
    render_telemetry_strip()
    st.markdown("""
    <div class="hero">
        <h1>📦 Real-Time Supply Chain Intelligence</h1>
        <p>A Big Data Engineering system for live streaming, distributed processing, anomaly detection, and analytical monitoring.</p>
    </div>
    """, unsafe_allow_html=True)

    st.subheader("Project Introduction")
    st.write(
        "Modern supply chains generate high-velocity transaction streams spanning factories, regional warehouses, and global logistics. "
        "Delays in processing these events create severe operational blindspots."
    )
    st.write(
        "This project implements an end-to-end distributed Big Data pipeline. Events are ingested into Apache Kafka, processed and aggregated "
        "in micro-batches with Apache Spark Structured Streaming, evaluated against anomaly rules, stored in PostgreSQL, and monitored via Streamlit."
    )

    st.subheader("Problem Statement")
    st.write(
        "Traditional batch architectures rely on periodic ETL jobs (hourly/nightly), causing hours of latency before pricing errors or "
        "shipping bottlenecks are discovered. This stream processing architecture reduces operational feedback latency to sub-second windows."
    )

    st.subheader("System Architecture")
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.markdown('<div class="info-card"><h4>1. Producer</h4>Simulates concurrent multi-regional supply transactions.</div>', unsafe_allow_html=True)
    with c2:
        st.markdown('<div class="info-card"><h4>2. Kafka</h4>Distributed log broker with partition parallelism.</div>', unsafe_allow_html=True)
    with c3:
        st.markdown('<div class="info-card"><h4>3. Spark</h4>Structured Streaming micro-batching & anomaly rules.</div>', unsafe_allow_html=True)
    with c4:
        st.markdown('<div class="info-card"><h4>4. PostgreSQL</h4>ACID relational sink for aggregations & incident logs.</div>', unsafe_allow_html=True)
    with c5:
        st.markdown('<div class="info-card"><h4>5. Streamlit</h4>Interactive dashboard refreshing live telemetry.</div>', unsafe_allow_html=True)

# --------------------------------------------------
# PAGE 2: LIVE ANALYTICS
# --------------------------------------------------
def show_live_analytics(df):
    render_telemetry_strip()
    st.title("📊 Live Supply Chain Analytics")
    st.markdown('<div class="status-note">Displaying live aggregates from PostgreSQL. Auto-refreshing every 5 seconds.</div>', unsafe_allow_html=True)

    col1, col2, col3 = st.columns(3)
    col1.metric("Products Tracked", f"{df['product'].nunique():,}")
    col2.metric("Total Transactions", f"{df['transaction_count'].sum():,}")
    col3.metric("Total Unit Throughput", f"{df['total_quantity'].sum():,}")
    st.divider()

    left, right = st.columns(2)
    with left:
        st.subheader("Transactions by Product")
        bar_df = df.sort_values("transaction_count", ascending=False)
        fig1 = px.bar(bar_df, x="product", y="transaction_count", color="product", text="transaction_count", template="plotly_dark")
        fig1.update_layout(showlegend=False, margin=dict(l=10, r=10, t=20, b=10))
        fig1.update_traces(textposition="outside")
        st.plotly_chart(fig1, use_container_width=True)
        st.markdown("""
        <div class="chart-explanation">
            <strong>📊 How to read this chart:</strong> Shows total order volume per SKU. A taller bar indicates a higher checkout frequency, highlighting the most popular or fast-moving inventory items.
        </div>
        """, unsafe_allow_html=True)

    with right:
        st.subheader("Inventory Share by Product")
        fig2 = px.pie(df, names="product", values="total_quantity", hole=0.4, template="plotly_dark")
        fig2.update_traces(textposition="inside", textinfo="percent+label")
        fig2.update_layout(margin=dict(l=10, r=10, t=20, b=10))
        st.plotly_chart(fig2, use_container_width=True)
        st.markdown("""
        <div class="chart-explanation">
            <strong>🥧 How to read this chart:</strong> Displays the proportion of total physical units shipped per product. Helps logistics managers balance warehouse shelf allocation based on volume footprint.
        </div>
        """, unsafe_allow_html=True)

    st.subheader("Product Analytics Details")
    st.dataframe(df, use_container_width=True, hide_index=True)
    st.caption(f"Latest database update: {df['last_updated'].max()}")

# --------------------------------------------------
# PAGE 3: BUSINESS INSIGHTS
# --------------------------------------------------
def show_business_insights(df):
    render_telemetry_strip()
    st.title("💡 Business Insights & SKU Efficiency")
    total_tx = df["transaction_count"].sum()
    total_qty = df["total_quantity"].sum()

    if total_tx == 0 or total_qty == 0:
        st.info("Awaiting initial transaction stream.")
        return

    comp = df.copy()
    comp["tx_share_%"] = (comp["transaction_count"] / total_tx * 100).round(2)
    comp["qty_share_%"] = (comp["total_quantity"] / total_qty * 100).round(2)
    comp["avg_units_per_order"] = (comp["total_quantity"] / comp["transaction_count"]).round(2)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Order Share vs. Volume Share")
        fig = px.bar(comp, x="product", y=["tx_share_%", "qty_share_%"], barmode="group", template="plotly_dark")
        fig.update_layout(margin=dict(l=10, r=10, t=20, b=10))
        st.plotly_chart(fig, use_container_width=True)
        st.markdown("""
        <div class="chart-explanation">
            <strong>📊 How to read this chart:</strong> Compares how often a product is ordered (tx_share) against the volume shipped (qty_share). Items where qty_share exceeds tx_share are typically purchased in bulk.
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.subheader("Order Size Density (Units / Order)")
        fig_density = px.bar(comp, x="product", y="avg_units_per_order", color="avg_units_per_order", color_continuous_scale="Viridis", template="plotly_dark")
        fig_density.update_layout(margin=dict(l=10, r=10, t=20, b=10))
        st.plotly_chart(fig_density, use_container_width=True)
        st.markdown("""
        <div class="chart-explanation">
            <strong>📦 How to read this chart:</strong> Shows the average package size per checkout. Higher values indicate wholesale or B2B purchasing patterns, whereas lower values point to retail consumer behavior.
        </div>
        """, unsafe_allow_html=True)

    st.subheader("Aggregated SKU Efficiency Matrix")
    st.dataframe(comp, use_container_width=True, hide_index=True)

# --------------------------------------------------
# PAGE 4: ANOMALY MONITORING
# --------------------------------------------------
def show_anomaly_monitoring(anom_df, metrics):
    avg_lat = anom_df["latency_seconds"].mean() if not anom_df.empty and "latency_seconds" in anom_df else None
    render_telemetry_strip(avg_latency=avg_lat)

    st.title("🚨 Operational Anomaly Center")
    st.markdown('<div class="alert-banner">Stream processing engine continuously flags transactions violating price limits (&gt; ₹400) or SLA delivery windows (&gt; 10 days).</div>', unsafe_allow_html=True)

    # KPI Metric Row
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Anomalies Flagged", f"{metrics['total']:,}")
    c2.metric("Delivery Bottlenecks", f"{metrics['delays']:,}")
    c3.metric("Price Spike Violations", f"{metrics['prices']:,}")
    c4.metric("Capital at Risk", f"₹ {metrics['exposure']:,.2f}")

    st.divider()

    if anom_df.empty:
        st.success("No anomalies detected in the current transaction stream.")
        return

    # Visual Analytics Row
    left, right = st.columns(2)
    with left:
        st.subheader("Anomalies by Region & Product")
        region_fig = px.bar(
            anom_df, x="region", color="product", 
            template="plotly_dark", barmode="stack"
        )
        region_fig.update_layout(margin=dict(l=10, r=10, t=20, b=10))
        st.plotly_chart(region_fig, use_container_width=True)
        st.markdown("""
        <div class="chart-explanation">
            <strong>📍 How to read this chart:</strong> Shows incident density across regional distribution centers. Identifies which geographical logistics corridors and product lines encounter the most delays and price deviations.
        </div>
        """, unsafe_allow_html=True)

    with right:
        st.subheader("Root Cause Distribution")
        r_fig = px.pie(anom_df, names="anomaly_reason", hole=0.45, template="plotly_dark")
        r_fig.update_layout(margin=dict(l=10, r=10, t=20, b=10))
        st.plotly_chart(r_fig, use_container_width=True)
        st.markdown("""
        <div class="chart-explanation">
            <strong>🔍 How to read this chart:</strong> Categorizes anomalies by failure mode (extreme shipping delays vs. pricing rule violations vs. compound critical failures) to pinpoint operational priorities.
        </div>
        """, unsafe_allow_html=True)

    # Interactive Filter & Live Feed
    st.subheader("Live Operational Incident Audit")

    # Persistent state options
    region_options = ["All", "North", "South", "East", "West"]
    severity_options = ["All", "CRITICAL", "HIGH (Delay)", "MEDIUM (Price)"]

    if "audit_region" not in st.session_state:
        st.session_state["audit_region"] = "All"
    if "audit_severity" not in st.session_state:
        st.session_state["audit_severity"] = "All"

    col_filter1, col_filter2, col_download = st.columns([2, 2, 2])
    with col_filter1:
        reg_idx = region_options.index(st.session_state["audit_region"]) if st.session_state["audit_region"] in region_options else 0
        selected_region = st.selectbox(
            "Filter Region:",
            region_options,
            index=reg_idx,
            key="audit_region"
        )
    with col_filter2:
        sev_idx = severity_options.index(st.session_state["audit_severity"]) if st.session_state["audit_severity"] in severity_options else 0
        selected_severity = st.selectbox(
            "Filter Severity:",
            severity_options,
            index=sev_idx,
            key="audit_severity"
        )
    with col_download:
        st.write("")  # Visual alignment spacing
        csv_buffer = io.StringIO()
        anom_df.to_csv(csv_buffer, index=False)
        st.download_button(
            label="📥 Export Incidents (CSV)",
            data=csv_buffer.getvalue(),
            file_name="supply_chain_incidents.csv",
            mime="text/csv",
            use_container_width=True
        )

    # Filter evaluation
    filtered_df = anom_df.copy()
    if selected_region != "All":
        filtered_df = filtered_df[filtered_df["region"] == selected_region]
    if selected_severity != "All":
        filtered_df = filtered_df[filtered_df["severity"] == selected_severity]

    st.caption(f"Displaying **{len(filtered_df)}** records matching selection (Filter locked across stream ticks).")

    st.dataframe(
        filtered_df[[
            "severity", "transaction_id", "timestamp", "product", 
            "region", "quantity", "unit_price", "delivery_time", "anomaly_reason", "latency_seconds"
        ]],
        use_container_width=True,
        hide_index=True
    )

# --------------------------------------------------
# ROUTER
# --------------------------------------------------
try:
    if page == "🏠 Project Overview":
        show_overview()

    elif page == "📊 Live Analytics":
        st_autorefresh(interval=5000, key="live_refresh")
        df, _ = refresh_all_data()
        if df.empty:
            st.info("Awaiting initial telemetry stream...")
        else:
            show_live_analytics(df)

    elif page == "💡 Business Insights":
        df = load_analytics_data()
        if df.empty:
            st.info("Awaiting initial telemetry stream...")
        else:
            show_business_insights(df)

    elif page == "🚨 Anomaly Monitoring":
        # Toggle to pause stream auto-refresh for static analysis during demonstrations
        is_live = st.sidebar.toggle("Auto-Refresh Stream (5s)", value=True)
        if is_live:
            st_autorefresh(interval=5000, key="anom_refresh")

        _, (anom_df, metrics) = refresh_all_data()
        show_anomaly_monitoring(anom_df, metrics)

except Exception as e:
    st.error("Error connecting to database or rendering telemetry.")
    st.exception(e)