import pytest

from anp.publicacao import executar_athena, sql_adicionar_particao


class AthenaFalso:
    """Imita o cliente Athena: devolve os estados da lista, um por consulta."""

    def __init__(self, estados: list[str], motivo: str = "") -> None:
        self.estados = estados
        self.motivo = motivo
        self.inicios: list[dict] = []
        self.consultas = 0
        self.paradas: list[str] = []

    def start_query_execution(self, **kwargs: object) -> dict:
        self.inicios.append(kwargs)
        return {"QueryExecutionId": "consulta-1"}

    def get_query_execution(self, QueryExecutionId: str) -> dict:
        estado = self.estados[min(self.consultas, len(self.estados) - 1)]
        self.consultas += 1
        status: dict = {"State": estado}
        if self.motivo:
            status["StateChangeReason"] = self.motivo
        return {"QueryExecution": {"QueryExecutionId": QueryExecutionId, "Status": status}}

    def stop_query_execution(self, QueryExecutionId: str) -> dict:
        self.paradas.append(QueryExecutionId)
        return {}


def test_sql_particao_exato():
    assert sql_adicionar_particao("anp_combustiveis", "precos_gasolina_etanol", "b", 2025, 1) == (
        "ALTER TABLE anp_combustiveis.precos_gasolina_etanol ADD IF NOT EXISTS "
        "PARTITION (ano='2025', mes='01') "
        "LOCATION 's3://b/processado/ano=2025/mes=01/'"
    )


def test_athena_sucesso():
    athena = AthenaFalso(["RUNNING", "SUCCEEDED"])
    assert executar_athena(athena, "SELECT 1", "wg", "db", espera_s=0) == "consulta-1"
    assert athena.inicios == [
        {
            "QueryString": "SELECT 1",
            "WorkGroup": "wg",
            "QueryExecutionContext": {"Database": "db"},
        }
    ]
    assert athena.consultas == 2


def test_athena_falha_com_motivo():
    athena = AthenaFalso(["RUNNING", "FAILED"], motivo="Table not found")
    with pytest.raises(RuntimeError, match="Table not found"):
        executar_athena(athena, "SELECT 1", "wg", "db", espera_s=0)


def test_athena_timeout():
    athena = AthenaFalso(["RUNNING"])
    with pytest.raises(TimeoutError, match="consulta-1"):
        executar_athena(athena, "SELECT 1", "wg", "db", timeout_s=0, espera_s=0)


def test_athena_timeout_para_consulta():
    athena = AthenaFalso(["QUEUED"])
    with pytest.raises(TimeoutError):
        executar_athena(athena, "SELECT 1", "wg", "db", timeout_s=0, espera_s=0)
    assert athena.paradas == ["consulta-1"]
