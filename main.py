import sys
from typing import Optional

from models import Schema
from database import Database, create_simulated_schema
from query import Query, JoinType
from optimizer import QueryOptimizer
from execution_plan import compare_plans


def print_header():
    print("=" * 70)
    print("  QUERY OPTIMIZER - Database Systems Lab 2")
    print("  Understanding Query Processing and Optimization")
    print("=" * 70)
    print()


def print_schema_info(schema: Schema):
    print("Loaded Schema:")
    print("-" * 50)
    
    for name, table in schema.tables.items():
        stats = schema.get_stats(name)
        indexes = stats.indexes if stats else []
        
        print(f"\n  {name}:")
        print(f"    Rows: {table.row_count:,}")
        print(f"    Columns: {', '.join(c.name for c in table.columns)}")
        
        if indexes:
            idx_strs = []
            for idx in indexes:
                if idx.is_composite:
                    idx_strs.append(f"{idx.name} ({', '.join(idx.columns)}) [COMPOSITE]")
                else:
                    idx_strs.append(f"{idx.name} ({', '.join(idx.columns)})")
            print(f"    Indexes: {'; '.join(idx_strs)}")
    
    print()


def get_sample_queries() -> list:
    queries = []
    
    q1 = Query()
    q1.select("c.id", "c.name", "c.email", "c.country")
    q1.from_table("customers", "c")
    q1.where("c.country", "=", "USA")
    queries.append(("Simple filter on customers", q1))
    
    q2 = Query()
    q2.select("c.name", "o.id", "o.total", "o.status")
    q2.from_table("customers", "c")
    q2.join("orders", "o", "c.id", "o.customer_id")
    q2.where("c.country", "=", "Germany")
    q2.where("o.status", "=", "delivered")
    queries.append(("Join with predicate pushdown", q2))
    
    q3 = Query()
    q3.select("c.name", "p.name", "oi.quantity")
    q3.from_table("customers", "c")
    q3.join("orders", "o", "c.id", "o.customer_id")
    q3.join("order_items", "oi", "o.id", "oi.order_id")
    q3.join("products", "p", "oi.product_id", "p.id")
    q3.where("c.country", "=", "USA")
    q3.limit(100)
    queries.append(("Multi-table join with LIMIT", q3))
    
    q4 = Query()
    q4.select("p.name", "p.price", "cat.name")
    q4.from_table("products", "p")
    q4.join("categories", "cat", "p.category_id", "cat.id")
    q4.where("p.price", ">", 500)
    q4.where("p.price", "<", 900)
    q4.order_by("p.price", desc=True)
    queries.append(("Range query with sort", q4))
    
    q5 = Query()
    q5.select("c.name", "c.country", "o.total", "p.name")
    q5.from_table("orders", "o")
    q5.join("customers", "c", "o.customer_id", "c.id")
    q5.join("order_items", "oi", "o.id", "oi.order_id")
    q5.join("products", "p", "oi.product_id", "p.id")
    q5.join("categories", "cat", "p.category_id", "cat.id")
    q5.where("c.country", "=", "Japan")
    q5.where("o.status", "=", "shipped")
    q5.where("cat.name", "=", "Category 5")
    q5.order_by("o.total", desc=True)
    q5.limit(50)
    queries.append(("Complex 5-table join", q5))
    
    return queries


def run_demo_mode(schema: Schema, optimizer: QueryOptimizer):
    print("\n" + "=" * 70)
    print("  DEMO MODE - Sample Query Optimization")
    print("=" * 70)
    
    queries = get_sample_queries()
    
    for i, (description, query) in enumerate(queries, 1):
        print(f"\n{'='*70}")
        print(f"  Query {i}: {description}")
        print("=" * 70)
        
        print("\nSQL:")
        print(query.to_sql())
        
        naive_plan = optimizer.build_naive_plan(query)
        optimized_plan = optimizer.optimize(query)
        
        print("\n" + compare_plans(naive_plan, optimized_plan))
        
        input("\nPress Enter for next query...")


def build_custom_query(schema: Schema) -> Optional[Query]:
    print("\n" + "-" * 50)
    print("Custom Query Builder")
    print("-" * 50)
    
    available_tables = list(schema.tables.keys())
    print(f"\nAvailable tables: {', '.join(available_tables)}")
    
    query = Query()
    
    cols = input("\nSELECT columns (comma-separated, or * for all): ").strip()
    if cols == "*":
        query.select("*")
    else:
        for col in cols.split(","):
            query.select(col.strip())
    
    from_table = input("FROM table (name or 'name alias'): ").strip()
    parts = from_table.split()
    if len(parts) == 2:
        query.from_table(parts[0], parts[1])
    else:
        query.from_table(parts[0])
    
    while True:
        join_input = input("JOIN (table alias left_col right_col) or Enter to skip: ").strip()
        if not join_input:
            break
        
        parts = join_input.split()
        if len(parts) >= 4:
            table, alias, left_col, right_col = parts[0], parts[1], parts[2], parts[3]
            query.join(table, alias, left_col, right_col)
        elif len(parts) >= 3:
            table, left_col, right_col = parts[0], parts[1], parts[2]
            query.join(table, None, left_col, right_col)
    
    while True:
        where_input = input("WHERE (column operator value) or Enter to skip: ").strip()
        if not where_input:
            break
        
        parts = where_input.split(maxsplit=2)
        if len(parts) >= 3:
            col, op, val = parts[0], parts[1], parts[2]
            try:
                val = float(val) if '.' in val else int(val)
            except ValueError:
                val = val.strip("'\"")
            query.where(col, op, val)
    
    order_input = input("ORDER BY (column [DESC]) or Enter to skip: ").strip()
    if order_input:
        parts = order_input.split()
        desc = len(parts) > 1 and parts[1].upper() == "DESC"
        query.order_by(parts[0], desc)
    
    limit_input = input("LIMIT (number) or Enter to skip: ").strip()
    if limit_input:
        try:
            query.limit(int(limit_input))
        except ValueError:
            pass
    
    return query


def interactive_mode(schema: Schema, optimizer: QueryOptimizer):
    while True:
        print("\n" + "=" * 70)
        print("  OPTIONS")
        print("=" * 70)
        print("  1. Run sample queries (demo)")
        print("  2. Build custom query")
        print("  3. Show schema info")
        print("  4. Exit")
        print()
        
        choice = input("Select option (1-4): ").strip()
        
        if choice == "1":
            run_demo_mode(schema, optimizer)
        
        elif choice == "2":
            query = build_custom_query(schema)
            if query:
                print("\n" + "-" * 50)
                print("Generated SQL:")
                print(query.to_sql())
                
                print("\n" + "-" * 50)
                print("Generating execution plans...")
                
                naive_plan = optimizer.build_naive_plan(query)
                optimized_plan = optimizer.optimize(query)
                
                print("\n" + compare_plans(naive_plan, optimized_plan))
        
        elif choice == "3":
            print_schema_info(schema)
        
        elif choice == "4":
            print("\nGoodbye!")
            break
        
        else:
            print("Invalid option. Please try again.")


def main():
    print_header()
    
    print("Attempting database connection...")
    db = Database()
    
    if db.connect():
        print("Connected to PostgreSQL!")
        schema = db.load_schema([
            "categories", "customers", "products", "orders", "order_items"
        ])
        db.close()
        
        if not schema.tables:
            print("No tables found. Using simulated schema.")
            schema = create_simulated_schema()
    else:
        print("Using simulated schema (no database connection).")
        schema = create_simulated_schema()
    
    print_schema_info(schema)
    
    optimizer = QueryOptimizer(schema)
    
    if len(sys.argv) > 1 and sys.argv[1] == "--demo":
        run_demo_mode(schema, optimizer)
    else:
        interactive_mode(schema, optimizer)


if __name__ == "__main__":
    main()
