from airflow.models import DAG
from airflow.utils.dates import days_ago

ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(6) # dias atrás para iniciar
}

with DAG(
    dag_id="tst_proj_dag",
    default_args=ARGS,
    schedule_interval="0 2 * * *", #executa todos os dias as 2 da manha
    max_active_runs=1 #executa uma instância por vez
    ) as dag:
    
  executa_raw = "executa tarefas da camada raw e salva num bucket-raw"

  executa_trusted = "executa tarefas da camada trusted e salva num bucket-trusted"

  executa_refined = "executa tarefas da camada refined e salva num bucket-refined"

  executa_raw >> executa_trusted >> executa_refined