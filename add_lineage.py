import datetime
import json
import os

import mysql.connector
from dotenv import load_dotenv
from google import genai
from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.entity.data.storedProcedure import StoredProcedure
from metadata.generated.schema.entity.data.table import Table
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    AuthProvider,
    OpenMetadataConnection,
)
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
    OpenMetadataJWTClientConfig,
)
from metadata.generated.schema.type.entityLineage import EntitiesEdge
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.ometa.ometa_api import OpenMetadata

# load các biến môi trường từ file .env
load_dotenv()
database_service_name = os.getenv("DATABASE_SERVICE_NAME")
ai_key = os.getenv("AI_KEY")


# hàm để lấy id bảng từ tên
def get_id(schema, table, another_schema=False):
    if another_schema:
        schema_list = [schema, schema.upper(), schema.lower(), schema.capitalize()]
        table_list = [table, table.upper(), table.lower(), table.capitalize()]
        for schema_type in schema_list:
            for table_type in table_list:
                entity = metadata.get_by_name(
                    entity=Table,
                    fqn=f"{database_service_name}.default.{schema_type}.{table_type}",
                )
                if entity is not None:
                    return entity.id.root
    else:
        table_list = [table, table.upper(), table.lower(), table.capitalize()]
        for table_type in table_list:
            entity = metadata.get_by_name(
                entity=Table,
                fqn=f"{database_service_name}.default.{schema}.{table_type}",
            )
            if entity is not None:
                return entity.id.root


# hàm để tạo lineage
def add_lineage(source_id, target_id):
    add_lineage_request = AddLineageRequest(
        edge=EntitiesEdge(
            fromEntity=EntityReference(id=source_id, type="table"),
            toEntity=EntityReference(id=target_id, type="table"),
        ),
    )

    metadata.add_lineage(data=add_lineage_request)


# tạo kết nối đến openmetadata
server_config = OpenMetadataConnection(
    hostPort="http://localhost:8585/api",
    authProvider=AuthProvider.openmetadata,
    securityConfig=OpenMetadataJWTClientConfig(
        jwtToken="eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImluZ2VzdGlvbi1ib3QiLCJyb2xlcyI6WyJJbmdlc3Rpb25Cb3RSb2xlIl0sImVtYWlsIjoiaW5nZXN0aW9uLWJvdEBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3NDAxMjAzNjgsImV4cCI6bnVsbH0.q1wRneLCQRBpmLa2bot0FW1oHg1VJb_RYA5cIVy3Oxb_jl6KdN04aD62R2PrCiZBbyQYeebZ9ibUHnjoXNl1eRh33XodbV3ZAUezP5yNDPhdnQaG3rMUP91bKoW8GMbsCWSR7w9nSh5pzVE3MBdxg48S5xLIGviNWpsiCu36akVbVaF_RzfH2bPdgVzLUtlIeSkNwc4-HdP65y3kOrb_UCKm-oUEHmpeIxfW2hgMuY5wGuwLRj_liim8CgN2Koi0B5NiEHT3CazjGsKjF9um2IXy596Y7cN4PIU0354mnWld9qXTXkCbhEy6sPhBjMmUbL9JIuwv4Qjvm5SUHspDyA",
    ),
)
metadata = OpenMetadata(server_config)

# tạo kết nối đến MySQL của openmetadata
conn = mysql.connector.connect(
    host="localhost",
    user="linh",
    password="03062004",
    database="log_ai",
)
cursor = conn.cursor()

# lấy ra tất cả các procedure từ database (đang test ở schema apps)
stored_procedures = metadata.list_entities(
    entity=StoredProcedure,
    params={"database": f"{database_service_name}.default"},
)

# gọi client gemini
client = genai.Client(api_key=ai_key)

# duyệt qua tất cả các procedure
for procedure in stored_procedures.entities:
    s_tables = []
    t_tables = []

    response = client.models.generate_content(
        model="gemini-2.0-flash",
        contents=f"Trích xuất danh sách bảng nguồn và bảng đích trong SQL procedure sau:\n\n{procedure.storedProcedureCode.code}\n\n"
        f"Trả về kết quả dưới dạng JSON với format:\n"
        f'{{"source_tables": [nếu bảng theo dạng schema.table thì trả về cả schema.table còn không thì trả về table thôi], "target_tables": [nếu bảng theo dạng schema.table thì trả về cả schema.table còn không thì trả về table thôi]}}',
    )

    # Thêm log của response từ client gemini vào database
    sql = "insert into log values (%s, %s)"
    data = (datetime.datetime.now(), response.text)
    cursor.execute(sql, data)
    conn.commit()

    # kiểm tra xem response có chứa "Không thể" không, nếu có bỏ qua luôn procedure đó
    not_found = ["Không thể", "không thể"]
    for nf in not_found:
        if nf in response.text:
            continue

    try:
        start = response.text.index("\n{") + 1
        end = response.text.index("\n}") + 2
    except Exception:
        continue
    cleaned_response = response.text[start:end].strip()
    try:
        parsed_response = json.loads(cleaned_response)
    except Exception:
        print(start, end)
        print("-----")
        print(procedure)
        cursor.close()
        conn.close()
        break
    s_tables = parsed_response.get("source_tables", [])
    t_tables = parsed_response.get("target_tables", [])

    # kiểm tra danh sách bảng nguồn và đích có rỗng không, nếu rỗng thì bỏ qua procedure đó
    if len(s_tables) == 0 or len(t_tables) == 0:
        continue

    print(s_tables)
    print(t_tables)
    print("-----")

    # duyệt qua tất cả các bảng nguồn và đích để tạo lineage
    for source in s_tables:
        for target in t_tables:
            source_split = source.split(".")
            target_split = target.split(".")
            try:
                if len(source_split) == 1:
                    source_table_id = get_id(
                        procedure.databaseSchema.name, source_split[0].upper()
                    )
                else:
                    source_table_id = get_id(
                        source_split[0],
                        source_split[1],
                        another_schema=True,
                    )
            except Exception as e:
                print(e)
                print(f"Cannot find source table {source_split}")
            try:
                if len(target_split) == 1:
                    target_table_id = get_id(
                        procedure.databaseSchema.name, target_split[0].upper()
                    )
                else:
                    target_table_id = get_id(
                        target_split[0],
                        target_split[1],
                        another_schema=True,
                    )
            except Exception as e:
                print(e)
                print(f"Cannot find target table {target_split}")
            add_lineage(source_id=source_table_id, target_id=target_table_id)
