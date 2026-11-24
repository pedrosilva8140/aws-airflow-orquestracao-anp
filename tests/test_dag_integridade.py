"""Integridade do DAG: só roda onde o Airflow está instalado (CI/Linux, EC2).

No Windows o Airflow não roda: o módulo é pulado. Com `EXIGIR_AIRFLOW=1` (definido no
CI), a ausência do Airflow é erro, para os testes nunca serem pulados em silêncio.

A checagem usa `airflow.sdk`, e não só `airflow`: a pasta `airflow/` da raiz do
repositório (Dockerfile e compose) é importável como pacote de namespace quando o
diretório atual está no `sys.path`, e enganaria um `importorskip("airflow")`.
"""

import os
from pathlib import Path

import pytest

if os.environ.get("EXIGIR_AIRFLOW") == "1":
    import airflow.sdk  # noqa: F401
else:
    pytest.importorskip("airflow.sdk")

from airflow.dag_processing.dagbag import DagBag  # noqa: E402

PASTA_DAGS = Path(__file__).resolve().parents[1] / "dags"
DAG_ID = "anp_precos_mensal"
ORDEM = [
    "descobrir_url",
    "enviar_para_p2",
    "aguardar_resultado",
    "checar_qualidade",
    "publicar_particao",
]


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(PASTA_DAGS))


@pytest.fixture(scope="module")
def dag(dagbag):
    return dagbag.dags[DAG_ID]


def test_sem_erros_de_import(dagbag):
    assert dagbag.import_errors == {}


def test_dag_existe(dagbag):
    assert DAG_ID in dagbag.dags


def test_cinco_tarefas_em_sequencia(dag):
    assert sorted(dag.task_ids) == sorted(ORDEM)
    assert dag.get_task(ORDEM[0]).upstream_task_ids == set()
    for anterior, seguinte in zip(ORDEM, ORDEM[1:], strict=False):
        assert dag.get_task(anterior).downstream_task_ids == {seguinte}
    assert dag.get_task(ORDEM[-1]).downstream_task_ids == set()


def test_agendamento(dag):
    assert dag.schedule == "0 9 15 * *"
    assert type(dag.timetable).__name__ == "CronTriggerTimetable"
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_retentativas_e_alerta(dag):
    for tarefa in dag.tasks:
        assert tarefa.retries == 3, tarefa.task_id
        assert tarefa.retry_exponential_backoff == 2.0, tarefa.task_id
        assert tarefa.on_failure_callback, tarefa.task_id


def test_sensor(dag):
    sensor = dag.get_task("aguardar_resultado")
    assert sensor.mode == "reschedule"
    assert sensor.poke_interval == 30
    assert sensor.timeout == 900
