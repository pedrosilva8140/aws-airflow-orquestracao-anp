"""Leitura do resultado do P2 para o arquivo de um mês.

O P2 grava `processado/.../<nome>.parquet` quando aceita o arquivo e
`rejeitado/<nome>.json` (com a chave `motivos`) quando rejeita. Num reprocessamento,
essas chaves podem já existir de uma execução anterior: por isso só contam objetos
com `LastModified` a partir do upload (`desde`).
"""

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from botocore.exceptions import ClientError

CODIGOS_AUSENTE = {"404", "NoSuchKey", "NotFound"}


PREFIXO_PROCESSADO = "processado/"


@dataclass(frozen=True)
class Resultado:
    """Resultado do P2 para o arquivo de um mês.

    `fora_do_mes`: o P2 aceitou o arquivo, mas não gravou nada na partição do mês
    (todas as linhas caíram em outras partições); as chaves encontradas vão em `saidas`.
    """

    status: Literal["pendente", "processado", "rejeitado", "fora_do_mes"]
    chave: str | None = None
    motivos: tuple[str, ...] = ()
    saidas: tuple[str, ...] = ()


def _nome(ano: int, mes: int) -> str:
    return f"precos-gasolina-etanol-{ano}-{mes:02d}"


def chave_parquet(ano: int, mes: int) -> str:
    return f"processado/ano={ano}/mes={mes:02d}/{_nome(ano, mes)}.parquet"


def chave_rejeicao(ano: int, mes: int) -> str:
    return f"rejeitado/{_nome(ano, mes)}.json"


def _recente(s3, bucket: str, chave: str, desde: datetime) -> bool:
    """True se o objeto existe e foi gravado a partir de `desde`.

    Só 404 conta como ausência. Um 403 (por exemplo, falta de `s3:ListBucket`) sobe,
    para não virar uma espera silenciosa até o timeout do sensor.
    """
    try:
        cabecalho = s3.head_object(Bucket=bucket, Key=chave)
    except ClientError as erro:
        if erro.response.get("Error", {}).get("Code") in CODIGOS_AUSENTE:
            return False
        raise
    return cabecalho["LastModified"] >= desde


def listar_saidas(s3, bucket: str, ano: int, mes: int, desde: datetime) -> list[str]:
    """Todas as saídas recentes do arquivo de `ano`/`mes`, em qualquer partição de `processado/`.

    O P2 particiona pela data de coleta de cada linha: linhas de outro mês viram um Parquet
    com o mesmo nome em outra partição. Devolve as chaves ordenadas.
    """
    sufixo = f"/{_nome(ano, mes)}.parquet"
    encontradas: list[str] = []
    argumentos = {"Bucket": bucket, "Prefix": PREFIXO_PROCESSADO}
    while True:
        pagina = s3.list_objects_v2(**argumentos)
        for objeto in pagina.get("Contents", []):
            if objeto["Key"].endswith(sufixo) and objeto["LastModified"] >= desde:
                encontradas.append(objeto["Key"])
        if not pagina.get("IsTruncated"):
            return sorted(encontradas)
        argumentos["ContinuationToken"] = pagina["NextContinuationToken"]


def verificar_resultado(s3, bucket: str, ano: int, mes: int, desde: datetime) -> Resultado:
    """Diz se o P2 já processou ou rejeitou o arquivo enviado em `desde`.

    Ordem: rejeição recente (tem prioridade), Parquet na partição do mês e, só se os dois
    faltarem, a listagem das outras partições (`fora_do_mes`).

    Atenção: o P2 grava as partições em ordem crescente, uma por vez. Um `fora_do_mes`
    pode ser só o meio da gravação (partição anterior já gravada, a do mês ainda não);
    quem chama deve confirmar num momento seguinte antes de tratar como definitivo.
    """
    rejeicao = chave_rejeicao(ano, mes)
    if _recente(s3, bucket, rejeicao, desde):
        corpo = s3.get_object(Bucket=bucket, Key=rejeicao)["Body"].read()
        motivos = json.loads(corpo.decode("utf-8")).get("motivos", [])
        return Resultado("rejeitado", rejeicao, tuple(str(m) for m in motivos))

    parquet = chave_parquet(ano, mes)
    if _recente(s3, bucket, parquet, desde):
        return Resultado("processado", parquet)

    saidas = listar_saidas(s3, bucket, ano, mes, desde)
    if saidas:
        return Resultado("fora_do_mes", saidas=tuple(saidas))

    return Resultado("pendente")
