"""Publicação da partição do mês no catálogo (Glue), via DDL no Athena.

`ADD IF NOT EXISTS` torna a publicação idempotente: reprocessar um mês não falha nem
duplica a partição (os dados ficam no mesmo prefixo, sobrescritos pelo P2).
"""

import time

ESTADOS_FINAIS_COM_ERRO = {"FAILED", "CANCELLED"}


def sql_adicionar_particao(database: str, tabela: str, bucket: str, ano: int, mes: int) -> str:
    return (
        f"ALTER TABLE {database}.{tabela} ADD IF NOT EXISTS "
        f"PARTITION (ano='{ano}', mes='{mes:02d}') "
        f"LOCATION 's3://{bucket}/processado/ano={ano}/mes={mes:02d}/'"
    )


def executar_athena(
    athena,
    sql: str,
    workgroup: str,
    database: str,
    timeout_s: int = 120,
    espera_s: float = 2.0,
) -> str:
    """Executa `sql` no Athena e espera o fim. Devolve o id da execução.

    FAILED/CANCELLED: `RuntimeError` com o motivo do Athena. Passou de `timeout_s`:
    a consulta é parada (para não seguir consumindo) e sobe `TimeoutError`.
    """
    execucao = athena.start_query_execution(
        QueryString=sql,
        WorkGroup=workgroup,
        QueryExecutionContext={"Database": database},
    )["QueryExecutionId"]

    inicio = time.monotonic()
    while True:
        status = athena.get_query_execution(QueryExecutionId=execucao)["QueryExecution"]["Status"]
        estado = status["State"]
        if estado == "SUCCEEDED":
            return execucao
        if estado in ESTADOS_FINAIS_COM_ERRO:
            motivo = status.get("StateChangeReason", "sem motivo informado")
            raise RuntimeError(f"Consulta {execucao} no Athena terminou em {estado}: {motivo}")
        if time.monotonic() - inicio >= timeout_s:
            athena.stop_query_execution(QueryExecutionId=execucao)
            raise TimeoutError(
                f"Consulta {execucao} no Athena passou de {timeout_s} s (último estado: {estado})"
            )
        time.sleep(espera_s)
