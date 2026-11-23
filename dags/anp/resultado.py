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


@dataclass(frozen=True)
class Resultado:
    status: Literal["pendente", "processado", "rejeitado"]
    chave: str | None = None
    motivos: tuple[str, ...] = ()


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


def verificar_resultado(s3, bucket: str, ano: int, mes: int, desde: datetime) -> Resultado:
    """Diz se o P2 já processou ou rejeitou o arquivo enviado em `desde`.

    A rejeição tem prioridade quando as duas saídas são recentes.
    """
    rejeicao = chave_rejeicao(ano, mes)
    if _recente(s3, bucket, rejeicao, desde):
        corpo = s3.get_object(Bucket=bucket, Key=rejeicao)["Body"].read()
        motivos = json.loads(corpo.decode("utf-8")).get("motivos", [])
        return Resultado("rejeitado", rejeicao, tuple(str(m) for m in motivos))

    parquet = chave_parquet(ano, mes)
    if _recente(s3, bucket, parquet, desde):
        return Resultado("processado", parquet)

    return Resultado("pendente")
