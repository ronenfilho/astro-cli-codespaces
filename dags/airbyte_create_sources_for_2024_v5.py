from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.decorators import task

# =============================================================================
# TAREFA 1: Criar a Fonte no Airbyte
# =============================================================================
@task
def create_airbyte_source(month: str, year: str = "2024") -> str:
    """
    Cria uma fonte no Airbyte para um mês e ano específicos.
    Retorna o sourceId gerado para uso na próxima tarefa.
    """
    import requests
    from airflow.utils.log.logging_mixin import LoggingMixin
    from airflow.hooks.base import BaseHook
    from airflow.models.variable import Variable

    log = LoggingMixin().log

    log.info(f"--- CRIANDO FONTE AIRBYTE PARA {year}-{month} ---")

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
        "name": f"DISPONIBILIDADE_USINA_{year_month} - ONS (AUT)",
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
        result = response.json()
        source_id = result.get('sourceId')
        
        if not source_id:
            raise ValueError("Resposta da API não contém sourceId")
            
        log.info(f"Fonte criada com SUCESSO! SourceId: {source_id}")
        return source_id
    else:
        log.error(f"Falha ao criar fonte. Status: {response.status_code}")
        log.error(f"Resposta da API: {response.text}")
        response.raise_for_status()

# =============================================================================
# TAREFA 2: Criar a Conexão entre Fonte e Destino
# =============================================================================
@task
def create_airbyte_connection(source_id: str) -> str:
    """
    Cria a conexão entre a fonte criada e o destino Snowflake.
    Retorna o connectionId gerado.
    """
    import requests
    from airflow.utils.log.logging_mixin import LoggingMixin
    from airflow.hooks.base import BaseHook
    from airflow.models.variable import Variable

    log = LoggingMixin().log

    if not source_id or source_id == "None":
        raise ValueError(f"source_id inválido: {source_id}")

    log.info(f"--- CRIANDO CONEXÃO PARA SOURCE_ID: {source_id} ---")

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

    destination_id = Variable.get("airbyte_destination_id_snowflake")
    
    # Validar se os IDs estão no formato UUID correto
    import re
    uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    
    if not re.match(uuid_pattern, source_id, re.IGNORECASE):
        raise ValueError(f"Source ID não está no formato UUID válido: {source_id}")
        
    if not re.match(uuid_pattern, destination_id, re.IGNORECASE):
        raise ValueError(f"Destination ID não está no formato UUID válido: {destination_id}")

    # Primeiro, vamos verificar se a fonte existe
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    # Verificar se a fonte existe
    source_endpoint = f"{base_url}/v1/sources/{source_id}"
    log.info(f"Verificando se a fonte existe: {source_endpoint}")
    
    source_response = requests.get(source_endpoint, headers=headers)
    if source_response.status_code != 200:
        log.error(f"Fonte não encontrada. Status: {source_response.status_code}")
        log.error(f"Resposta: {source_response.text}")
        raise ValueError(f"Fonte {source_id} não existe ou não pode ser acessada")
    
    log.info("Fonte verificada com sucesso!")

    # Configura a conexão para ser 'manual' e 'ativa'
    payload = {
        "sourceId": source_id,
        "destinationId": destination_id,
        "status": "active",
        "scheduleType": "manual"  # Mudança aqui - scheduleType direto, não aninhado
    }

    endpoint = f"{base_url}/v1/connections"
    
    log.info(f"Enviando requisição POST para: {endpoint}")
    log.info(f"Source ID: {source_id}")
    log.info(f"Destination ID: {destination_id}")
    log.info(f"Payload: {payload}")

    response = requests.post(endpoint, headers=headers, json=payload)

    if response.status_code == 200:
        result = response.json()
        connection_id = result.get('connectionId')
        
        if not connection_id:
            raise ValueError("Resposta da API não contém connectionId")
            
        log.info(f"Conexão criada com SUCESSO! ConnectionId: {connection_id}")
        return connection_id
    else:
        log.error(f"Falha ao criar conexão. Status: {response.status_code}")
        log.error(f"Resposta da API: {response.text}")
        
        # Tentar payload alternativo se o primeiro falhar
        if response.status_code == 403:
            log.info("Tentando payload alternativo...")
            payload_alt = {
                "sourceId": source_id,
                "destinationId": destination_id,
                "status": "active",
                "schedule": {
                    "scheduleType": "manual"
                }
            }
            
            response_alt = requests.post(endpoint, headers=headers, json=payload_alt)
            if response_alt.status_code == 200:
                result = response_alt.json()
                connection_id = result.get('connectionId')
                log.info(f"Conexão criada com payload alternativo! ConnectionId: {connection_id}")
                return connection_id
            else:
                log.error(f"Payload alternativo também falhou. Status: {response_alt.status_code}")
                log.error(f"Resposta: {response_alt.text}")
        
        response.raise_for_status()

# =============================================================================
# DEFINIÇÃO DA DAG
# =============================================================================
with DAG(
    dag_id="airbyte_create_sources_for_2024_v5",
    start_date=pendulum.datetime(2025, 9, 25, tz="America/Sao_Paulo"),
    schedule=None,
    catchup=False,
    tags=["airbyte", "automation", "Inteligencia Energetica"],
    doc_md="""
    ### DAG para Criação de Fontes e Conexões no Airbyte - v5
    
    Esta DAG automatiza a criação de fontes e conexões no Airbyte para todos os meses de 2024.
    
    **Fluxo de Trabalho:**
    1. **TAREFA 1**: Cria uma fonte no Airbyte para cada mês
    2. **TAREFA 2**: Cria uma conexão entre cada fonte e o destino Snowflake
    
    **Recursos:**
    - Busca arquivos de disponibilidade de usinas do ONS
    - Configuração manual de sincronização (não automática)
    - Conexões ficam ativas mas precisam ser disparadas manualmente
    
    **Pré-requisitos:**
    1. Conexão HTTP `airbyte_api` configurada no Airflow
    2. Variáveis configuradas no Airflow:
       - `airbyte_workspace_id`
       - `airbyte_source_definition_id`
       - `airbyte_destination_id_snowflake`
    """
) as dag:
    
    # Gera a lista de meses ('01', '02', ..., '12')
    months = [f"{m:02d}" for m in range(1, 13)]
    
    # Etapa 1: Mapeamento dinâmico para criar as fontes
    source_ids = create_airbyte_source.expand(month=months)
    
    # Etapa 2: Mapeamento dinâmico para criar as conexões,
    # dependendo da criação das fontes
    create_airbyte_connection.expand(source_id=source_ids)