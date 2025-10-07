from __future__ import annotations

import logging
import pendulum
import requests
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.models.variable import Variable
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)

# =============================================================================
# TAREFA 1: Criar a Fonte no Airbyte
# =============================================================================
@task
def create_airbyte_source(month: str, year: str = "2024") -> str:
    """Cria a fonte de dados e retorna o sourceId gerado."""
    
    # Validação de parâmetros
    if not month or not month.isdigit() or not (1 <= int(month) <= 12):
        raise AirflowException(f"Mês inválido: {month}. Deve ser entre 01 e 12.")
    
    if not year or not year.isdigit():
        raise AirflowException(f"Ano inválido: {year}. Deve ser numérico.")

    try:
        conn = BaseHook.get_connection('airbyte_api')
        api_key = conn.password
        base_url = conn.host
        workspace_id = Variable.get("airbyte_workspace_id")
        definition_id = Variable.get("airbyte_source_definition_id")
        year_month = f"{year}_{month}"
        
        payload = {
            "name": f"DISPONIBILIDADE_USINA_{year_month} - ONS (AUT)",
            "workspaceId": workspace_id,
            "definitionId": definition_id,
            "configuration": {
                "format": "csv",
                "provider": { "storage": "HTTPS" },
                "url": f"https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/disponibilidade_usina_ho/DISPONIBILIDADE_USINA_{year_month}.csv",
                "dataset_name": f"DISPONIBILIDADE_USINA_{year_month}",
                "reader_options": "{ \"delimiter\": \";\" }"
            }
        }
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        endpoint = f"{base_url}/v1/sources"

        logger.info(f"Criando fonte para {year_month}...")
        response = requests.post(endpoint, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        
        source_id = response.json().get('sourceId')
        if not source_id:
            raise AirflowException("Resposta da API não contém sourceId")
            
        logger.info(f"Fonte {source_id} criada com sucesso!")
        return source_id
        
    except requests.exceptions.RequestException as e:
        raise AirflowException(f"Erro na requisição HTTP: {str(e)}")
    except KeyError as e:
        raise AirflowException(f"Variável não encontrada: {str(e)}")
    except Exception as e:
        raise AirflowException(f"Erro inesperado ao criar fonte: {str(e)}")

# =============================================================================
# TAREFA 2: Criar a Conexão entre Fonte e Destino
# =============================================================================
@task
def create_airbyte_connection(source_id: str) -> str:
    """Cria a conexão para um source_id e retorna o connectionId gerado."""
    
    if not source_id:
        raise AirflowException("source_id não pode ser vazio")

    try:
        conn = BaseHook.get_connection('airbyte_api')
        api_key = conn.password
        base_url = conn.host
        destination_id = Variable.get("airbyte_destination_id_snowflake")

        # Configura a conexão para ser 'manual' e 'ativa'
        payload = {
            "sourceId": source_id,
            "destinationId": destination_id,
            "status": "active",
            "schedule": {"scheduleType": "manual"}
        }
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        endpoint = f"{base_url}/v1/connections"

        logger.info(f"Criando conexão para a fonte {source_id}...")
        response = requests.post(endpoint, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        
        connection_id = response.json().get('connectionId')
        if not connection_id:
            raise AirflowException("Resposta da API não contém connectionId")
            
        logger.info(f"Conexão {connection_id} criada com sucesso!")
        return connection_id
        
    except requests.exceptions.RequestException as e:
        raise AirflowException(f"Erro na requisição HTTP: {str(e)}")
    except KeyError as e:
        raise AirflowException(f"Variável não encontrada: {str(e)}")
    except Exception as e:
        raise AirflowException(f"Erro inesperado ao criar conexão: {str(e)}")

# =============================================================================
# DEFINIÇÃO DA DAG
# =============================================================================
with DAG(
    dag_id="airbyte_create_sources_2024_dag_v5",  # Corrigido para corresponder ao nome do arquivo
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),  # Ajustado para 2024
    schedule=None,
    catchup=False,
    tags=["airbyte", "automation", "Inteligencia Energetica"],
    doc_md="""
    ### Automação de Fontes e Conexões no Airbyte
    Esta DAG cria uma fonte e uma conexão para cada mês de 2024.
    A sincronização de cada conexão deve ser acionada manualmente.
    """,
) as dag:
    
    # Gera a lista de meses ('01', '02', ..., '12')
    months = [f"{m:02d}" for m in range(1, 13)]
    
    # Etapa 1: Mapeamento dinâmico para criar as fontes
    source_ids = create_airbyte_source.expand(month=months)
    
    # Etapa 2: Mapeamento dinâmico para criar as conexões,
    # dependendo da criação das fontes
    create_airbyte_connection.expand(source_id=source_ids)