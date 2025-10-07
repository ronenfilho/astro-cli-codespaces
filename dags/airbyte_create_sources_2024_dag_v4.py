from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.decorators import task
# HttpHook e Variable foram removidos daqui

@task
def generate_months_for_2024() -> list[str]:
    """Gera uma lista com os números dos meses de 01 a 12."""
    return [f"{month:02d}" for month in range(1, 13)]

@task
def create_airbyte_source_for_month(month: str, year: str = "2024"):
    """
    Cria uma fonte no Airbyte para um mês e ano específicos, fazendo uma chamada à API
    DIRETAMENTE com a biblioteca 'requests' para bypassar o HttpHook.
    """
    import requests
    from airflow.utils.log.logging_mixin import LoggingMixin
    from airflow.hooks.base import BaseHook
    from airflow.models.variable import Variable # Mantido aqui dentro

    log = LoggingMixin().log

    log.info("--- INICIANDO TESTE FINAL COM REQUESTS DIRETOS ---")

    try:
        conn = BaseHook.get_connection('airbyte_api')
        api_key = conn.password
        base_url = conn.host

        if not api_key:
            raise ValueError("O campo 'Password' na conexão 'airbyte_api' está vazio!")
        
        log.info(f"Chave de API recuperada. Início: {api_key[:8]}...")

    except Exception as e:
        log.error(f"Falha ao obter a conexão 'airbyte_api'. Erro: {e}")
        raise

    workspace_id = Variable.get("airbyte_workspace_id")
    definition_id = Variable.get("airbyte_source_definition_id")
    
    year_month = f"{year}_{month}"

    payload = {
        "name": f"DISPONIBILIDADE_USINA_{year_month} - ONS (Direct Requests)",
        "workspaceId": workspace_id,
        "definitionId": definition_id,
        "configuration": {
            "format": "csv",
            "provider": { "storage": "HTTPS", "user_agent": False },
            "url": f"https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/disponibilidade_usina_ho/DISPONIBILIDADE_USINA_{year_month}.csv",
            "dataset_name": f"DISPONIBILIDADE_USINA_{year_month}",
            "reader_options": "{ \"delimiter\": \";\" }"
        }
    }

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    endpoint = f"{base_url}/v1/sources"
    
    log.info(f"Enviando requisição POST para: {endpoint}")

    response = requests.post(endpoint, headers=headers, json=payload)

    if response.status_code == 200:
        log.info("Fonte criada com SUCESSO usando requests diretos!")
        log.info(response.json())
        return response.json()
    else:
        log.error(f"Falha ao criar fonte com requests diretos. Status: {response.status_code}")
        log.error(f"Resposta da API: {response.text}")
        response.raise_for_status()

with DAG(
    dag_id="airbyte_create_sources_for_2024_v4",
    start_date=pendulum.datetime(2025, 9, 25, tz="America/Sao_Paulo"),
    schedule=None,
    catchup=False,
    tags=["airbyte", "setup", "Inteligencia Energetica"],
    doc_md="""
    ### DAG para Criação de Fontes no Airbyte
    
    Esta DAG cria fontes no Airbyte para todos os meses do ano de 2024.
    Ela busca os arquivos de disponibilidade de usinas do ONS.
    
    **Pré-requisitos:**
    1. Conexão HTTP `airbyte_api` configurada no Airflow.
    2. Variáveis `airbyte_workspace_id` e `airbyte_source_definition_id` criadas no Airflow.
    """
) as dag:
    months_list = generate_months_for_2024()
    create_airbyte_source_for_month.expand(month=months_list)
