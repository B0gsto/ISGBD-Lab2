import psycopg2
from database import get_connection_params

conn = psycopg2.connect(**get_connection_params())
cur = conn.cursor()

# Check the complex query
cur.execute("""
    SELECT COUNT(*) FROM orders o
    JOIN customers c ON o.customer_id = c.id
    JOIN order_items oi ON o.id = oi.order_id
    JOIN products p ON oi.product_id = p.id
    JOIN categories cat ON p.category_id = cat.id
    WHERE c.country = 'Japan' AND o.status = 'shipped' AND cat.name = 'Category 5'
""")
print('Japan + shipped + Category 5:', cur.fetchone()[0])

# Find combinations that work
cur.execute("""
    SELECT c.country, o.status, cat.name, COUNT(*) as cnt
    FROM orders o
    JOIN customers c ON o.customer_id = c.id
    JOIN order_items oi ON o.id = oi.order_id
    JOIN products p ON oi.product_id = p.id
    JOIN categories cat ON p.category_id = cat.id
    GROUP BY c.country, o.status, cat.name
    HAVING COUNT(*) > 100
    ORDER BY cnt DESC
    LIMIT 10
""")
print("\nTop combinations (country, status, category):")
for row in cur.fetchall():
    print(f"  {row[0]:12} | {row[1]:12} | {row[2]:15} | {row[3]}")

conn.close()
