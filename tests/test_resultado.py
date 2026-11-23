import json
from datetime import UTC, datetime, timedelta

import pytest
from botocore.exceptions import ClientError
from conftest import BUCKET

from anp.resultado import Resultado, chave_parquet, chave_rejeicao, verificar_resultado

DESDE = datetime(2026, 11, 15, 12, 0, tzinfo=UTC)
DEPOIS = DESDE + timedelta(minutes=2)
ANTES = DESDE - timedelta(hours=1)
PARQUET = chave_parquet(2026, 10)
REJEICAO = chave_rejeicao(2026, 10)


def json_rejeicao(motivos: list[str]) -> bytes:
    relatorio = {
        "arquivo_original": "entrada/precos-gasolina-etanol-2026-10.csv",
        "arquivo_rejeitado": "rejeitado/precos-gasolina-etanol-2026-10.csv",
        "motivos": motivos,
        "data_rejeicao": DEPOIS.isoformat(),
    }
    return json.dumps(relatorio, ensure_ascii=False, indent=2).encode("utf-8")


def test_chaves_no_formato_do_p2():
    assert PARQUET == "processado/ano=2026/mes=10/precos-gasolina-etanol-2026-10.parquet"
    assert REJEICAO == "rejeitado/precos-gasolina-etanol-2026-10.json"
    assert chave_parquet(2025, 1).endswith("/mes=01/precos-gasolina-etanol-2025-01.parquet")


def test_pendente_sem_objetos(s3):
    assert verificar_resultado(s3, BUCKET, 2026, 10, DESDE) == Resultado("pendente")


def test_processado_recente(s3):
    s3.gravar(PARQUET, b"PAR1", DEPOIS)
    assert verificar_resultado(s3, BUCKET, 2026, 10, DESDE) == Resultado("processado", PARQUET)


def test_ignora_parquet_antigo(s3):
    s3.gravar(PARQUET, b"PAR1", ANTES)
    assert verificar_resultado(s3, BUCKET, 2026, 10, DESDE) == Resultado("pendente")


def test_rejeitado_com_motivos(s3):
    motivos = [
        "valor invalido em \"Data da Coleta\" (1 linha(s)): linha 3 ('31/02/2026')",
        "coluna ausente: Bandeira",
    ]
    s3.gravar(REJEICAO, json_rejeicao(motivos), DEPOIS)
    resultado = verificar_resultado(s3, BUCKET, 2026, 10, DESDE)
    assert resultado == Resultado("rejeitado", REJEICAO, tuple(motivos))


def test_rejeicao_tem_prioridade(s3):
    s3.gravar(PARQUET, b"PAR1", DEPOIS)
    s3.gravar(REJEICAO, json_rejeicao(["motivo"]), DEPOIS)
    assert verificar_resultado(s3, BUCKET, 2026, 10, DESDE).status == "rejeitado"


def test_rejeicao_antiga_nao_vence_parquet_recente(s3):
    s3.gravar(PARQUET, b"PAR1", DEPOIS)
    s3.gravar(REJEICAO, json_rejeicao(["de uma execucao anterior"]), ANTES)
    assert verificar_resultado(s3, BUCKET, 2026, 10, DESDE) == Resultado("processado", PARQUET)


def test_aceita_last_modified_igual_a_desde(s3):
    s3.gravar(PARQUET, b"PAR1", DESDE)
    assert verificar_resultado(s3, BUCKET, 2026, 10, DESDE).status == "processado"


def test_erro_de_permissao_propaga(s3):
    s3.negados.add(PARQUET)
    with pytest.raises(ClientError):
        verificar_resultado(s3, BUCKET, 2026, 10, DESDE)
