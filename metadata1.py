from metadata.generated.schema.api.services.createDatabaseService import (
    CreateDatabaseServiceRequest,
)
from metadata.generated.schema.api.services.ingestionPipelines.createIngestionPipeline import (
    CreateIngestionPipelineRequest,
)
from metadata.generated.schema.entity.services.connections.database.common.basicAuth import (
    BasicAuth,
)
from metadata.generated.schema.entity.services.connections.database.mysqlConnection import (
    MysqlConnection,
)
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    AuthProvider,
    OpenMetadataConnection,
)
from metadata.generated.schema.entity.services.databaseService import (
    DatabaseConnection,
    DatabaseService,
    DatabaseServiceType,
)
from metadata.generated.schema.entity.services.ingestionPipelines.ingestionPipeline import (
    AirflowConfig,
    IngestionPipeline,
    PipelineType,
)
from metadata.generated.schema.metadataIngestion.databaseServiceMetadataPipeline import (
    DatabaseServiceMetadataPipeline,
)
from metadata.generated.schema.metadataIngestion.workflow import SourceConfig
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
    OpenMetadataJWTClientConfig,
)
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.ometa.ometa_api import OpenMetadata

server_config = OpenMetadataConnection(
    hostPort="http://localhost:8585/api",
    authProvider=AuthProvider.openmetadata,
    securityConfig=OpenMetadataJWTClientConfig(
        jwtToken="eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImluZ2VzdGlvbi1ib3QiLCJyb2xlcyI6WyJJbmdlc3Rpb25Cb3RSb2xlIl0sImVtYWlsIjoiaW5nZXN0aW9uLWJvdEBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3Mzk0MjQ2MTIsImV4cCI6bnVsbH0.cAjh1-3r4Mdu-zkQJso42HpOgdRRvcGodTcVr-duRgAPO9CM3QNabA4cLnaL7aZ3yJxdLvGAD84bjlqo9DunDumlHF-YdEjMQE-1zQOHwahjJra7RFirBkSdVJeEcbJKHu_BPFqzbmkBOqdCBxpTaBhLi--vGnxYzAo4r_6xrmlnXxmbZFDD1fOWLaLoRRx-y40W6E5m5ekbm0ntHpw_izp6BVyVQAssLzzUuJQViy6gCMNG0ooE2ifwWL0MngSlUYCCt4UNzLuO29p9EEqRXD4XMsUq746Ns-zXKp2-isUnkaBEgBBHgLgnHr2MpMcXOudZI8ug7cKapSqT0E1o_g",
    ),
)
metadata = OpenMetadata(server_config)

create_service = CreateDatabaseServiceRequest(
    name="test-service-table1",
    serviceType=DatabaseServiceType.Mysql,
    connection=DatabaseConnection(
        config=MysqlConnection(
            username="linh",
            authType=BasicAuth(password="03062004"),
            hostPort="mysql:3306",
            databaseName="TEST",
        )
    ),
)
service_entity = metadata.create_or_update(data=create_service)
database_services = metadata.list_services(DatabaseService)
ingestion_pipeline = CreateIngestionPipelineRequest(
    name="test-pipeline",
    service=EntityReference(id=service_entity.id, type="databaseService"),
    pipelineType=PipelineType.metadata,
    sourceConfig=SourceConfig(
        config=DatabaseServiceMetadataPipeline(
            markDeletedTables=True,
            includeTables=True,
            includeViews=True,
        )
    ),
    airflowConfig=AirflowConfig(scheduleInterval="0 0 * * *"),
)
metadata.create_or_update(data=ingestion_pipeline)

a = metadata.get_by_name(entity=DatabaseService, fqn="test-service-table1")

b = metadata.get_by_name(
    entity=IngestionPipeline, fqn="test-service-table1.test-pipeline"
)
print(b.id.root)

metadata.run_pipeline(ingestion_pipeline_id=b.id.root)
