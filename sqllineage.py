import json

import requests
import sqlparse

from sqllineage.runner import LineageRunner

# Headers và url
headers = {
    "Content-Type": "application/json",
    "Authorization": "Bearer eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImxpbmhsdW9ubGVvMDQiLCJyb2xlcyI6WyJBZG1pbiJdLCJlbWFpbCI6ImxpbmhsdW9ubGVvMDRAZ21haWwuY29tIiwiaXNCb3QiOmZhbHNlLCJ0b2tlblR5cGUiOiJQRVJTT05BTF9BQ0NFU1MiLCJpYXQiOjE3MzUzMTI2OTUsImV4cCI6MTc0MzA4ODY5NX0.nYYcdJl1T8a1sv9uFSln_XyUYbgs7oLNj0_pCahxNBK4VIIqX01dY2DFtXZJGuPh_dgUyt62-L3Mj0u1xGhoWZ_25jo6ka3p11RZrdzFVT2DpJFZQZOWod-9BJ39vjXe67Mi1VKuGHiUOQ21_WN0FMjxB3mahAU1pbVME9XyVJLRWWLBTWcL5CvGIs3oIX4Z1s5A41ya06S5tdH5GpApuG8p5iwsrwkXNMWHceb-8e52Gr8GvT-rKOjr9Hh_Pgya4N_tqK9jnJopAdztS1vn7npS2OqZ-ppC5w9MmxA1bzfPUABTUlyEyb3n3zGhJOv7-SwTFBFgWx78QFmnxafTow",
}
url = "http://localhost:8585/api/v1"


# Hàm lấy id của bảng
def get_id(schema_name, table_name, url):
    response = requests.get(
        f"{url}/tables/name/TEST.mysql.{schema_name}.{table_name}", headers=headers
    )
    return response.json()["id"]


sql = """
CREATE TABLE mysql.customer_order_summary AS
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.phone,
    c.address,
    c.city,
    c.state,
    c.zip_code,
    o.order_id,
    o.order_date,
    o.total_amount,
    oi.product_id,
    p.product_name,
    oi.quantity,
    oi.price AS product_price,
    (oi.quantity * oi.price) AS total_product_amount,
    pay.payment_date,
    pay.amount AS payment_amount,
    pay.payment_method,
    pay.payment_status
FROM mysql.customers c
JOIN mysql.orders o ON c.customer_id = o.customer_id
JOIN mysql.order_items oi ON o.order_id = oi.order_id
JOIN mysql.products p ON oi.product_id = p.product_id
LEFT JOIN mysql.payments pay ON o.order_id = pay.order_id;
"""
statements = sqlparse.split(sql)
for statement in statements:
    result = LineageRunner(statement)

    # Lấy thông tin các bảng
    source_tables = [
        {
            "schema": str(res)[: str(res).index(".")],
            "table": str(res)[str(res).index(".") + 1 :],
        }
        for res in result.source_tables
    ]
    target_tables = [
        {
            "schema": str(res)[: str(res).index(".")],
            "table": str(res)[str(res).index(".") + 1 :],
        }
        for res in result.target_tables
    ]

    # Lấy id của các bảng
    source_tables_id = [
        get_id(source_table["schema"], source_table["table"], url)
        for source_table in source_tables
    ]
    target_tables_id = [
        get_id(target_table["schema"], target_table["table"], url)
        for target_table in target_tables
    ]

    # Tạo các lineage từ id vừa lấy
    payload = {}

    for source in source_tables_id:
        for target in target_tables_id:
            edge = {
                "fromEntity": {"id": source, "type": "table"},
                "toEntity": {"id": target, "type": "table"},
            }
            payload = {"edge": edge}
            response = requests.put(
                f"{url}/lineage", headers=headers, data=json.dumps(payload)
            )
