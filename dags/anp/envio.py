"""Download do arquivo mensal da ANP e envio para o prefixo `entrada/` do P2.

O nome no S3 é padronizado por ano e mês: reprocessar o mesmo mês sobrescreve o
mesmo objeto, e a idempotência por nome do P2 evita dados duplicados no backfill.
"""

import tempfile
from collections.abc import Callable
from datetime import UTC, datetime

import requests

TAMANHO_MINIMO = 1000
BYTES_INICIO = 64 * 1024
PEDACO_DOWNLOAD = 1024 * 1024
MEMORIA_MAXIMA = 16 * 1024 * 1024
TIMEOUT_DOWNLOAD_S = 60


class ArquivoInvalido(Exception):
    """O conteúdo baixado não parece o CSV da ANP (HTML de erro, arquivo vazio, etc.)."""


def chave_entrada(ano: int, mes: int) -> str:
    """Chave padronizada do arquivo no prefixo `entrada/` do P2."""
    return f"entrada/precos-gasolina-etanol-{ano}-{mes:02d}.csv"


def _decodificar(linha: bytes) -> str:
    try:
        return linha.decode("utf-8-sig")
    except UnicodeDecodeError:
        return linha.decode("latin-1")


def validar_csv_basico(inicio: bytes, tamanho_total: int) -> None:
    """Barra o que claramente não é o CSV da ANP, antes de chegar ao P2.

    `inicio` são os primeiros bytes do arquivo (o suficiente para a primeira linha).
    """
    if tamanho_total < TAMANHO_MINIMO:
        raise ArquivoInvalido(
            f"Arquivo com {tamanho_total} bytes, abaixo do mínimo de {TAMANHO_MINIMO}."
        )
    primeira_linha = _decodificar(inicio.split(b"\n", 1)[0]).strip()
    if ";" not in primeira_linha or "Data da Coleta" not in primeira_linha:
        raise ArquivoInvalido(
            f"A primeira linha não é o cabeçalho do CSV da ANP: {primeira_linha[:120]!r}"
        )


def enviar(
    s3,
    bucket: str,
    url: str,
    chave: str,
    baixar: Callable[..., requests.Response] = requests.get,
) -> datetime:
    """Baixa `url` em streaming, valida e envia para `s3://bucket/chave`.

    Devolve o `LastModified` (UTC) do objeto enviado: o sensor só aceita resultados
    do P2 gravados a partir desse instante.
    """
    with tempfile.SpooledTemporaryFile(max_size=MEMORIA_MAXIMA) as arquivo:
        with baixar(url, stream=True, timeout=TIMEOUT_DOWNLOAD_S) as resposta:
            resposta.raise_for_status()
            for pedaco in resposta.iter_content(chunk_size=PEDACO_DOWNLOAD):
                arquivo.write(pedaco)

        tamanho = arquivo.tell()
        arquivo.seek(0)
        validar_csv_basico(arquivo.read(BYTES_INICIO), tamanho)

        arquivo.seek(0)
        s3.upload_fileobj(arquivo, bucket, chave)

    cabecalho = s3.head_object(Bucket=bucket, Key=chave)
    return cabecalho["LastModified"].astimezone(UTC)
