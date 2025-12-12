import streamlit as st
import pandas as pd
from models import Schema
from database import Database, create_simulated_schema, get_connection_params
from query import Query
from optimizer import QueryOptimizer
from execution_plan import compare_plans

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

st.set_page_config(
    page_title="Query Optimizer",
    page_icon="🔍",
    layout="wide"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0;
    }
    .sub-header {
        color: #666;
        margin-top: 0;
    }
    .stCodeBlock {
        background-color: #1e1e1e;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def load_schema():
    """Load schema from database or use simulated data."""
    db = Database()
    if db.connect():
        schema = db.load_schema([
            "categories", "customers", "products", "orders", "order_items"
        ])
        db.close()
        if not schema.tables:
            schema = create_simulated_schema()
            return schema, False
        return schema, True
    return create_simulated_schema(), False


def execute_query(sql: str, limit: int = 100):
    """Execute SQL query and return results as DataFrame."""
    if not HAS_PSYCOPG2:
        return None, "psycopg2 not installed"
    
    try:
        params = get_connection_params()
        conn = psycopg2.connect(**params)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Add LIMIT if not present
        sql_lower = sql.lower()
        if 'limit' not in sql_lower:
            sql = sql.rstrip(';') + f" LIMIT {limit}"
        
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        if rows:
            df = pd.DataFrame(rows)
            return df, None
        return pd.DataFrame(), None
    except Exception as e:
        return None, str(e)


def get_sample_queries():
    """Return sample queries for demonstration."""
    queries = []
    
    # Query 1: Simple filter
    q1 = Query()
    q1.select("c.id", "c.name", "c.email", "c.country")
    q1.from_table("customers", "c")
    q1.where("c.country", "=", "USA")
    queries.append(("🔍 Simple filter on customers", q1))
    
    # Query 2: Join with predicate pushdown
    q2 = Query()
    q2.select("c.name", "o.id", "o.total", "o.status")
    q2.from_table("customers", "c")
    q2.join("orders", "o", "c.id", "o.customer_id")
    q2.where("c.country", "=", "France")
    q2.where("o.status", "=", "delivered")
    queries.append(("🔗 Join with predicate pushdown", q2))
    
    # Query 3: Multi-table join
    q3 = Query()
    q3.select("c.name AS customer_name", "p.name AS product_name", "oi.quantity")
    q3.from_table("customers", "c")
    q3.join("orders", "o", "c.id", "o.customer_id")
    q3.join("order_items", "oi", "o.id", "oi.order_id")
    q3.join("products", "p", "oi.product_id", "p.id")
    q3.where("c.country", "=", "USA")
    q3.limit(100)
    queries.append(("📦 Multi-table join with LIMIT", q3))
    
    # Query 4: Range query
    q4 = Query()
    q4.select("p.name AS product_name", "p.price", "cat.name AS category_name")
    q4.from_table("products", "p")
    q4.join("categories", "cat", "p.category_id", "cat.id")
    q4.where("p.price", ">", 500)
    q4.where("p.price", "<", 900)
    q4.order_by("p.price", desc=True)
    queries.append(("📊 Range query with sort", q4))
    
    # Query 5: Complex join
    q5 = Query()
    q5.select("c.name AS customer_name", "c.country", "o.total", "p.name AS product_name")
    q5.from_table("orders", "o")
    q5.join("customers", "c", "o.customer_id", "c.id")
    q5.join("order_items", "oi", "o.id", "oi.order_id")
    q5.join("products", "p", "oi.product_id", "p.id")
    q5.join("categories", "cat", "p.category_id", "cat.id")
    q5.where("c.country", "=", "Japan")
    q5.where("o.status", "=", "shipped")
    q5.where("cat.name", "=", "Category 38")
    q5.order_by("o.total", desc=True)
    q5.limit(50)
    queries.append(("🚀 Complex 5-table join", q5))
    
    return queries


def display_schema_info(schema: Schema):
    """Display schema information in a nice format."""
    cols = st.columns(len(schema.tables))
    
    for i, (name, table) in enumerate(schema.tables.items()):
        stats = schema.get_stats(name)
        indexes = stats.indexes if stats else []
        
        with cols[i]:
            st.markdown(f"### 📋 {name}")
            st.metric("Rows", f"{table.row_count:,}")
            
            st.markdown("**Columns:**")
            for col in table.columns:
                st.markdown(f"- `{col.name}`")
            
            if indexes:
                st.markdown("**Indexes:**")
                for idx in indexes:
                    if idx.is_composite:
                        st.markdown(f"- 🔗 `{idx.name}`")
                    else:
                        st.markdown(f"- 🔑 `{idx.name}`")


def main():
    # Header
    st.markdown('<h1 class="main-header">🔍 Query Optimizer</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Database Systems Lab 2 - Understanding Query Processing and Optimization</p>', unsafe_allow_html=True)
    
    # Load schema
    schema, connected = load_schema()
    optimizer = QueryOptimizer(schema)
    
    # Connection status
    if connected:
        st.success("✅ Connected to PostgreSQL database")
    else:
        st.warning("⚠️ Using simulated schema (no database connection)")
    
    # Tabs
    tab1, tab2, tab3 = st.tabs(["📊 Sample Queries", "✏️ Custom Query", "📋 Schema Info"])
    
    with tab1:
        st.header("Sample Queries")
        
        queries = get_sample_queries()
        query_names = [name for name, _ in queries]
        
        selected = st.selectbox("Select a query:", query_names)
        
        # Find selected query
        query = None
        for name, q in queries:
            if name == selected:
                query = q
                break
        
        if query:
            st.subheader("SQL Query")
            sql = query.to_sql()
            st.code(sql, language="sql")
            
            # Execute query and show results
            if connected:
                if st.button("▶️ Execute Query", key="sample_exec"):
                    with st.spinner("Executing query..."):
                        df, error = execute_query(sql)
                        if error:
                            st.error(f"Error: {error}")
                        elif df is not None:
                            st.success(f"✅ Query returned {len(df)} rows")
                            st.dataframe(df, use_container_width=True)
            else:
                st.info("💡 Connect to database to execute queries and see results")
            
            st.divider()
            st.subheader("Execution Plans")
            
            naive_plan = optimizer.build_naive_plan(query)
            optimized_plan = optimizer.optimize(query)
            
            plan_tab1, plan_tab2, plan_tab3 = st.tabs(["📈 Comparison", "❌ Naive Plan", "✅ Optimized Plan"])
            
            with plan_tab1:
                comparison = compare_plans(naive_plan, optimized_plan)
                st.text(comparison)
            
            with plan_tab2:
                st.text(naive_plan.format())
            
            with plan_tab3:
                st.text(optimized_plan.format())
    
    with tab2:
        st.header("Build Custom Query")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # SELECT
            st.subheader("SELECT")
            columns = st.text_input("Columns (comma-separated, or * for all)", value="*")
            
            # FROM
            st.subheader("FROM")
            available_tables = list(schema.tables.keys())
            from_table = st.selectbox("Table", available_tables)
            from_alias = st.text_input("Alias (optional)", value=from_table[0])
            
            # JOINs
            st.subheader("JOINs")
            num_joins = st.number_input("Number of joins", min_value=0, max_value=5, value=0)
            
            joins = []
            for i in range(int(num_joins)):
                st.markdown(f"**Join {i+1}**")
                jcol1, jcol2 = st.columns(2)
                with jcol1:
                    join_table = st.selectbox(f"Table", available_tables, key=f"join_table_{i}")
                    join_alias = st.text_input(f"Alias", value=join_table[0], key=f"join_alias_{i}")
                with jcol2:
                    left_col = st.text_input(f"Left column", key=f"left_col_{i}")
                    right_col = st.text_input(f"Right column", key=f"right_col_{i}")
                joins.append((join_table, join_alias, left_col, right_col))
        
        with col2:
            # WHERE
            st.subheader("WHERE")
            num_wheres = st.number_input("Number of conditions", min_value=0, max_value=5, value=0)
            
            wheres = []
            for i in range(int(num_wheres)):
                wcol1, wcol2, wcol3 = st.columns([2, 1, 2])
                with wcol1:
                    w_col = st.text_input(f"Column", key=f"where_col_{i}")
                with wcol2:
                    w_op = st.selectbox(f"Op", ["=", ">", "<", ">=", "<=", "!="], key=f"where_op_{i}")
                with wcol3:
                    w_val = st.text_input(f"Value", key=f"where_val_{i}")
                wheres.append((w_col, w_op, w_val))
            
            # ORDER BY
            st.subheader("ORDER BY")
            order_col = st.text_input("Column (optional)")
            order_desc = st.checkbox("Descending")
            
            # LIMIT
            st.subheader("LIMIT")
            limit_val = st.number_input("Limit (0 = no limit)", min_value=0, value=0)
        
        # Build and run query
        if st.button("🚀 Build & Optimize Query", type="primary"):
            try:
                query = Query()
                
                # SELECT
                if columns.strip() == "*":
                    query.select("*")
                else:
                    for col in columns.split(","):
                        query.select(col.strip())
                
                # FROM
                query.from_table(from_table, from_alias if from_alias else None)
                
                # JOINs
                for join_table_name, join_alias_name, left_col, right_col in joins:
                    if left_col and right_col:
                        query.join(join_table_name, join_alias_name, left_col, right_col)
                
                # WHERE
                for w_col, w_op, w_val in wheres:
                    if w_col and w_val:
                        # Try to convert to number
                        try:
                            val = float(w_val) if '.' in w_val else int(w_val)
                        except ValueError:
                            val = w_val
                        query.where(w_col, w_op, val)
                
                # ORDER BY
                if order_col:
                    query.order_by(order_col, order_desc)
                
                # LIMIT
                if limit_val > 0:
                    query.limit(limit_val)
                
                st.success("Query built successfully!")
                
                st.subheader("Generated SQL")
                sql = query.to_sql()
                st.code(sql, language="sql")
                
                # Execute and show results
                if connected:
                    with st.spinner("Executing query..."):
                        df, error = execute_query(sql)
                        if error:
                            st.error(f"Error: {error}")
                        elif df is not None:
                            st.success(f"✅ Query returned {len(df)} rows")
                            st.dataframe(df, use_container_width=True)
                else:
                    st.info("💡 Connect to database to execute queries and see results")
                
                st.divider()
                st.subheader("Execution Plans")
                naive_plan = optimizer.build_naive_plan(query)
                optimized_plan = optimizer.optimize(query)
                
                plan_col1, plan_col2 = st.columns(2)
                
                with plan_col1:
                    st.markdown("### ❌ Naive Plan")
                    st.text(naive_plan.format())
                
                with plan_col2:
                    st.markdown("### ✅ Optimized Plan")
                    st.text(optimized_plan.format())
                
                st.subheader("📊 Comparison")
                st.text(compare_plans(naive_plan, optimized_plan))
                
            except Exception as e:
                st.error(f"Error building query: {e}")
    
    with tab3:
        st.header("Database Schema")
        display_schema_info(schema)


if __name__ == "__main__":
    main()
