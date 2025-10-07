from datetime import datetime
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.decorators import task

# DAG principal que orquestra todo o pipeline
with DAG(
    dag_id="pipeline_completo_laboratorio",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["pipeline", "orchestration", "laboratorio"],
    description="Pipeline completo: Airbyte → dbt Staging → dbt Core",
    doc_md="""
    ## Pipeline Completo do Laboratório 3
    
    Este DAG orquestra todo o pipeline de dados:
    
    1. **Airbyte**: Carrega dados brutos para o staging
    2. **dbt Staging**: Processa e limpa os dados brutos
    3. **dbt Core**: Cria dimensões e tabelas de fatos
    
    ### Execução Sequencial:
    - Airbyte carrega dados → dbt staging processa → dbt core cria star schema
    
    ### Dependências:
    - dbt staging depende do Airbyte terminar
    - dbt core depende do dbt staging terminar
    """
) as dag:

    # Trigger do DAG do Airbyte - tolerante a falhas parciais
    trigger_airbyte = TriggerDagRunOperator(
        task_id="trigger_airbyte_sources",
        trigger_dag_id="airbyte_create_sources_for_2025_v6",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success', 'failed'],  # Permitir falhas parciais
    )

    @task
    def validate_airbyte_results():
        """
        Valida se o Airbyte teve sucesso suficiente para prosseguir com dbt.
        Aceita falhas parciais se pelo menos 70% das tasks funcionaram.
        """
        from airflow.models import DagRun, TaskInstance
        from airflow.utils.state import TaskInstanceState
        from airflow.utils.log.logging_mixin import LoggingMixin
        from airflow.utils.session import provide_session
        
        log = LoggingMixin().log
        
        @provide_session
        def get_latest_dag_run_and_tasks(session=None):
            # Buscar a execução mais recente do DAG do Airbyte
            dag_run = session.query(DagRun).filter(
                DagRun.dag_id == "airbyte_create_sources_for_2025_v6"
            ).order_by(DagRun.execution_date.desc()).first()
            
            if not dag_run:
                raise Exception("Nenhuma execução encontrada para o DAG do Airbyte")
            
            log.info(f"Verificando execução: {dag_run.run_id} - Estado: {dag_run.state}")
            
            # Buscar todas as task instances da execução
            task_instances = session.query(TaskInstance).filter(
                TaskInstance.dag_id == "airbyte_create_sources_for_2025_v6",
                TaskInstance.run_id == dag_run.run_id
            ).all()
            
            return dag_run, task_instances
        
        latest_run, task_instances = get_latest_dag_run_and_tasks()
        
        # Contar sucessos e falhas
        total_tasks = len(task_instances)
        success_tasks = len([ti for ti in task_instances if ti.state == TaskInstanceState.SUCCESS])
        failed_tasks = len([ti for ti in task_instances if ti.state == TaskInstanceState.FAILED])
        
        success_rate = (success_tasks / total_tasks) * 100 if total_tasks > 0 else 0
        
        log.info(f"📊 Resultados do Airbyte:")
        log.info(f"   Total tasks: {total_tasks}")
        log.info(f"   Sucessos: {success_tasks}")
        log.info(f"   Falhas: {failed_tasks}")
        log.info(f"   Taxa de sucesso: {success_rate:.1f}%")
        
        # Threshold de 70% de sucesso
        if success_rate >= 70:
            log.info("✅ Airbyte teve sucesso suficiente para prosseguir com dbt!")
            return "proceed"
        else:
            log.error(f"❌ Airbyte teve muitas falhas ({success_rate:.1f}% < 70%)")
            raise Exception(f"Taxa de sucesso do Airbyte muito baixa: {success_rate:.1f}%")

    # Validação dos resultados do Airbyte
    validate_airbyte = validate_airbyte_results()

    # Trigger do dbt staging após Airbyte completar e validação passar
    trigger_dbt_staging = TriggerDagRunOperator(
        task_id="trigger_dbt_staging",
        trigger_dag_id="dbt_staging_models", 
        wait_for_completion=True,
        poke_interval=30,
    )

    # Trigger do dbt core após staging completar
    trigger_dbt_core = TriggerDagRunOperator(
        task_id="trigger_dbt_core",
        trigger_dag_id="dbt_core_models",
        wait_for_completion=True,
        poke_interval=30,
    )

    # Definir dependências com validação
    trigger_airbyte >> validate_airbyte >> trigger_dbt_staging >> trigger_dbt_core