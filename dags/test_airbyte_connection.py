from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.decorators import task

# =============================================================================
# TESTE DE CONEXÃO COM AIRBYTE API
# =============================================================================
@task
def test_airbyte_connection():
    """
    Testa a conexão básica com a API do Airbyte
    """
    import requests
    from airflow.utils.log.logging_mixin import LoggingMixin
    from airflow.hooks.base import BaseHook
    from airflow.models.variable import Variable

    log = LoggingMixin().log

    log.info("=== TESTE DE CONEXÃO COM AIRBYTE API ===")

    try:
        # Testar conexão
        conn = BaseHook.get_connection('airbyte_api')
        api_key = conn.password
        base_url = conn.host
        client_id = conn.extra_dejson.get('client_id') if conn.extra_dejson else None

        log.info(f"Base URL: {base_url}")
        log.info(f"API Key: {api_key[:8]}..." if api_key else "API Key não configurada")
        log.info(f"Client ID: {client_id[:8]}..." if client_id else "Client ID não configurado")

        if not api_key:
            raise ValueError("API Key não configurada na conexão 'airbyte_api'")

        # Testar variáveis
        workspace_id = Variable.get("airbyte_workspace_id", default_var=None)
        definition_id = Variable.get("airbyte_source_definition_id", default_var=None)
        destination_id = Variable.get("airbyte_destination_id_snowflake", default_var=None)

        log.info(f"Workspace ID: {workspace_id}")
        log.info(f"Source Definition ID: {definition_id}")
        log.info(f"Destination ID: {destination_id}")

        if not all([workspace_id, definition_id, destination_id]):
            missing = []
            if not workspace_id: missing.append("airbyte_workspace_id")
            if not definition_id: missing.append("airbyte_source_definition_id")
            if not destination_id: missing.append("airbyte_destination_id_snowflake")
            raise ValueError(f"Variáveis não configuradas: {missing}")

        # Primeiro, obter token OAuth usando Client ID e Secret
        oauth_endpoint = f"{base_url}/v1/applications/token"
        log.info(f"Obtendo token OAuth: {oauth_endpoint}")
        
        oauth_payload = {
            "client_id": client_id,
            "client_secret": api_key
        }
        
        oauth_headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        oauth_response = requests.post(oauth_endpoint, headers=oauth_headers, json=oauth_payload, timeout=30)
        log.info(f"Status OAuth: {oauth_response.status_code}")
        
        if oauth_response.status_code != 200:
            log.error(f"Falha na autenticação OAuth: {oauth_response.text}")
            raise Exception(f"OAuth failed: {oauth_response.status_code} - {oauth_response.text}")
        
        oauth_data = oauth_response.json()
        access_token = oauth_data.get('access_token')
        
        if not access_token:
            log.error(f"Token não encontrado na resposta: {oauth_data}")
            raise Exception("Access token não retornado pela API OAuth")
            
        log.info(f"✅ Token OAuth obtido com sucesso: {access_token[:10]}...")

        # Agora testar chamada para API usando o token OAuth
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        # Teste 1: Verificar se o workspace existe
        workspace_endpoint = f"{base_url}/v1/workspaces/{workspace_id}"
        log.info(f"Testando workspace endpoint: {workspace_endpoint}")

        response = requests.get(workspace_endpoint, headers=headers, timeout=30)
        
        log.info(f"Status da resposta: {response.status_code}")
        log.info(f"Resposta da API: {response.text[:500]}")

        if response.status_code == 200:
            log.info("✅ SUCESSO: Conexão com Airbyte funcionando!")
            log.info("✅ Workspace acessível!")
            return "SUCCESS"
        else:
            log.error(f"❌ ERRO: Status {response.status_code}")
            log.error(f"Resposta: {response.text}")
            raise Exception(f"Falha na API: {response.status_code} - {response.text}")

    except Exception as e:
        log.error(f"❌ ERRO no teste de conexão: {e}")
        raise

# =============================================================================
# DEFINIÇÃO DA DAG
# =============================================================================
with DAG(
    dag_id="test_airbyte_connection",
    start_date=pendulum.datetime(2025, 10, 7, tz="America/Sao_Paulo"),
    schedule=None,
    catchup=False,
    tags=["airbyte", "test", "debug"],
    doc_md="""
    ### DAG de Teste - Conexão Airbyte
    
    Esta DAG testa a conexão básica com a API do Airbyte.
    
    **Objetivo:**
    - Verificar se a conexão `airbyte_api` está configurada corretamente
    - Testar se as variáveis estão definidas
    - Fazer uma chamada simples para a API do Airbyte
    """
) as dag:
    
    # Teste de conexão simples
    test_airbyte_connection()