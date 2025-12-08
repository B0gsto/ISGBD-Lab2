import os
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

from models import (
    Table, Column, Index, ColumnStats, TableStats, Schema, DataType
)

load_dotenv()


def get_connection_params() -> dict:
    return {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': int(os.getenv('POSTGRES_PORT', 5432)),
        'database': os.getenv('POSTGRES_DB', 'query_optimizer'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD', ''),
    }


class Database:
    def __init__(self, connection_params: Optional[dict] = None):
        self.params = connection_params or get_connection_params()
        self.conn = None
    
    def connect(self) -> bool:
        if not HAS_PSYCOPG2:
            print("Warning: psycopg2 not installed. Using simulated statistics.")
            return False
        
        try:
            self.conn = psycopg2.connect(**self.params)
            return True
        except psycopg2.Error as e:
            print(f"Database connection failed: {e}")
            return False
    
    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def get_table_info(self, table_name: str) -> Optional[Table]:
        if not self.conn:
            return None
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT column_name, data_type, is_nullable, 
                           column_default
                    FROM information_schema.columns
                    WHERE table_name = %s
                    ORDER BY ordinal_position
                """, (table_name,))
                
                columns = []
                for row in cur.fetchall():
                    dtype = self._map_data_type(row['data_type'])
                    columns.append(Column(
                        name=row['column_name'],
                        data_type=dtype,
                        nullable=row['is_nullable'] == 'YES',
                        is_primary_key='nextval' in str(row['column_default'] or '')
                    ))
                
                if not columns:
                    return None
                
                cur.execute(f"""
                    SELECT 
                        reltuples::bigint AS row_count,
                        relpages AS pages
                    FROM pg_class
                    WHERE relname = %s
                """, (table_name,))
                
                stats = cur.fetchone()
                row_count = int(stats['row_count']) if stats else 0
                pages = int(stats['pages']) if stats else 0
                
                return Table(
                    name=table_name,
                    columns=columns,
                    row_count=max(row_count, 0),
                    total_pages=pages,
                    avg_row_size=100 if pages == 0 else (pages * 8192) // max(row_count, 1)
                )
        
        except psycopg2.Error as e:
            print(f"Error fetching table info: {e}")
            return None
    
    def get_column_stats(self, table_name: str) -> Dict[str, ColumnStats]:
        if not self.conn:
            return {}
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        attname AS column_name,
                        n_distinct,
                        null_frac,
                        most_common_vals,
                        most_common_freqs
                    FROM pg_stats
                    WHERE tablename = %s
                """, (table_name,))
                
                result = {}
                for row in cur.fetchall():
                    distinct = row['n_distinct']
                    if distinct is not None and distinct < 0:
                        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                        count = cur.fetchone()['count']
                        distinct = int(abs(distinct) * count)
                    
                    result[row['column_name']] = ColumnStats(
                        distinct_count=int(distinct) if distinct else 0,
                        null_fraction=float(row['null_frac'] or 0),
                    )
                
                return result
        
        except psycopg2.Error as e:
            print(f"Error fetching column stats: {e}")
            return {}
    
    def get_indexes(self, table_name: str) -> List[Index]:
        if not self.conn:
            return []
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        i.relname AS index_name,
                        a.attname AS column_name,
                        ix.indisunique AS is_unique,
                        ix.indisprimary AS is_primary,
                        i.reltuples::bigint AS cardinality,
                        i.relpages AS pages,
                        array_position(ix.indkey, a.attnum) AS col_position
                    FROM pg_index ix
                    JOIN pg_class t ON t.oid = ix.indrelid
                    JOIN pg_class i ON i.oid = ix.indexrelid
                    JOIN pg_attribute a ON a.attrelid = t.oid 
                        AND a.attnum = ANY(ix.indkey)
                    WHERE t.relname = %s
                    ORDER BY i.relname, col_position
                """, (table_name,))
                
                indexes_dict: Dict[str, dict] = {}
                for row in cur.fetchall():
                    name = row['index_name']
                    if name not in indexes_dict:
                        indexes_dict[name] = {
                            'name': name,
                            'table_name': table_name,
                            'columns': [],
                            'is_unique': row['is_unique'],
                            'is_primary': row['is_primary'],
                            'cardinality': int(row['cardinality'] or 0),
                            'pages': int(row['pages'] or 0)
                        }
                    indexes_dict[name]['columns'].append(row['column_name'])
                
                return [Index(**data) for data in indexes_dict.values()]
        
        except psycopg2.Error as e:
            print(f"Error fetching indexes: {e}")
            return []
    
    def get_table_stats(self, table_name: str) -> Optional[TableStats]:
        table = self.get_table_info(table_name)
        if not table:
            return None
        
        column_stats = self.get_column_stats(table_name)
        indexes = self.get_indexes(table_name)
        
        return TableStats(
            table=table,
            column_stats=column_stats,
            indexes=indexes
        )
    
    def load_schema(self, table_names: List[str]) -> Schema:
        schema = Schema()
        
        for name in table_names:
            stats = self.get_table_stats(name)
            if stats:
                schema.add_table(stats.table, stats)
        
        return schema
    
    def _map_data_type(self, pg_type: str) -> DataType:
        pg_type = pg_type.lower()
        if 'int' in pg_type:
            return DataType.INTEGER
        elif 'char' in pg_type or 'text' in pg_type:
            return DataType.VARCHAR
        elif 'numeric' in pg_type or 'decimal' in pg_type:
            return DataType.DECIMAL
        elif 'timestamp' in pg_type or 'date' in pg_type:
            return DataType.TIMESTAMP
        else:
            return DataType.VARCHAR


def create_simulated_schema() -> Schema:
    schema = Schema()
    
    categories = Table(
        name="categories",
        columns=[
            Column("id", DataType.SERIAL, nullable=False, is_primary_key=True),
            Column("name", DataType.VARCHAR, nullable=False),
            Column("description", DataType.TEXT, nullable=True),
        ],
        row_count=100,
        avg_row_size=150,
        total_pages=2
    )
    categories_stats = TableStats(
        table=categories,
        column_stats={
            "id": ColumnStats(distinct_count=100, null_fraction=0.0),
            "name": ColumnStats(distinct_count=100, null_fraction=0.0),
        },
        indexes=[
            Index("categories_pkey", "categories", ["id"], is_unique=True, is_primary=True, cardinality=100)
        ]
    )
    schema.add_table(categories, categories_stats)
    
    customers = Table(
        name="customers",
        columns=[
            Column("id", DataType.SERIAL, nullable=False, is_primary_key=True),
            Column("name", DataType.VARCHAR, nullable=False),
            Column("email", DataType.VARCHAR, nullable=False),
            Column("country", DataType.VARCHAR, nullable=True),
            Column("created_at", DataType.TIMESTAMP, nullable=True),
        ],
        row_count=10000,
        avg_row_size=200,
        total_pages=250
    )
    customers_stats = TableStats(
        table=customers,
        column_stats={
            "id": ColumnStats(distinct_count=10000, null_fraction=0.0),
            "name": ColumnStats(distinct_count=9500, null_fraction=0.0),
            "email": ColumnStats(distinct_count=10000, null_fraction=0.0),
            "country": ColumnStats(distinct_count=10, null_fraction=0.0),
        },
        indexes=[
            Index("customers_pkey", "customers", ["id"], is_unique=True, is_primary=True, cardinality=10000)
        ]
    )
    schema.add_table(customers, customers_stats)
    
    products = Table(
        name="products",
        columns=[
            Column("id", DataType.SERIAL, nullable=False, is_primary_key=True),
            Column("name", DataType.VARCHAR, nullable=False),
            Column("category_id", DataType.INTEGER, nullable=True),
            Column("price", DataType.DECIMAL, nullable=False),
            Column("stock_quantity", DataType.INTEGER, nullable=True),
        ],
        row_count=5000,
        avg_row_size=180,
        total_pages=110
    )
    products_stats = TableStats(
        table=products,
        column_stats={
            "id": ColumnStats(distinct_count=5000, null_fraction=0.0),
            "name": ColumnStats(distinct_count=5000, null_fraction=0.0),
            "category_id": ColumnStats(distinct_count=100, null_fraction=0.0),
            "price": ColumnStats(distinct_count=1000, null_fraction=0.0, min_value=1.0, max_value=1000.0),
        },
        indexes=[
            Index("products_pkey", "products", ["id"], is_unique=True, is_primary=True, cardinality=5000),
            Index("idx_products_category", "products", ["category_id"], cardinality=100)
        ]
    )
    schema.add_table(products, products_stats)
    
    orders = Table(
        name="orders",
        columns=[
            Column("id", DataType.SERIAL, nullable=False, is_primary_key=True),
            Column("customer_id", DataType.INTEGER, nullable=True),
            Column("order_date", DataType.TIMESTAMP, nullable=True),
            Column("total", DataType.DECIMAL, nullable=True),
            Column("status", DataType.VARCHAR, nullable=True),
        ],
        row_count=50000,
        avg_row_size=120,
        total_pages=750
    )
    orders_stats = TableStats(
        table=orders,
        column_stats={
            "id": ColumnStats(distinct_count=50000, null_fraction=0.0),
            "customer_id": ColumnStats(distinct_count=10000, null_fraction=0.0),
            "status": ColumnStats(distinct_count=4, null_fraction=0.0),
            "total": ColumnStats(distinct_count=10000, null_fraction=0.0, min_value=10.0, max_value=1010.0),
        },
        indexes=[
            Index("orders_pkey", "orders", ["id"], is_unique=True, is_primary=True, cardinality=50000),
            Index("idx_orders_customer", "orders", ["customer_id"], cardinality=10000)
        ]
    )
    schema.add_table(orders, orders_stats)
    
    order_items = Table(
        name="order_items",
        columns=[
            Column("id", DataType.SERIAL, nullable=False, is_primary_key=True),
            Column("order_id", DataType.INTEGER, nullable=True),
            Column("product_id", DataType.INTEGER, nullable=True),
            Column("quantity", DataType.INTEGER, nullable=False),
            Column("unit_price", DataType.DECIMAL, nullable=False),
        ],
        row_count=150000,
        avg_row_size=80,
        total_pages=1500
    )
    order_items_stats = TableStats(
        table=order_items,
        column_stats={
            "id": ColumnStats(distinct_count=150000, null_fraction=0.0),
            "order_id": ColumnStats(distinct_count=50000, null_fraction=0.0),
            "product_id": ColumnStats(distinct_count=5000, null_fraction=0.0),
            "quantity": ColumnStats(distinct_count=10, null_fraction=0.0),
        },
        indexes=[
            Index("order_items_pkey", "order_items", ["id"], is_unique=True, is_primary=True, cardinality=150000),
            Index("idx_orderitems_order_product", "order_items", ["order_id", "product_id"], cardinality=150000)
        ]
    )
    schema.add_table(order_items, order_items_stats)
    
    return schema
