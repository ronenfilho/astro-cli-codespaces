from datetime import datetime
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# Configuração do projeto dbt
project_config = ProjectConfig(
    dbt_project_path="/usr/local/airflow/include/laboratorio_dbt",
)

# Configuração do perfil de conexão
profile_config = ProfileConfig(
    profile_name="laboratorio_dbt",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_dev", # usa a Connection criada manualmente no Airflow
        profile_args={
            "database": "LAB_PIPELINE",
            "schema": "STAGING",
            "warehouse": "LAB_WH_DBT",
            "role": "DBT_DEV",
        },
    ),
)

# Configuração de renderização para staging
render_config_staging = RenderConfig(
    select=["path:models/staging"]
)

# Configuração de renderização para core  
render_config_core = RenderConfig(
    select=["path:models/core"]
)

# DAG para executar modelos de staging
staging_dag = DbtDag(
    dag_id="dbt_staging_models",
    project_config=project_config,
    profile_config=profile_config,
    render_config=render_config_staging,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dbt", "staging", "data-pipeline"],
    description="Executa modelos dbt da camada de staging",
)

# DAG para executar modelos de core
core_dag = DbtDag(
    dag_id="dbt_core_models",
    project_config=project_config,
    profile_config=profile_config,
    render_config=render_config_core,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dbt", "core", "data-pipeline"],
    description="Executa modelos dbt da camada de core (dimensões e fatos)",
)