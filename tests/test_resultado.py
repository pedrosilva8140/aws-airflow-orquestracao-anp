import json
from datetime import UTC, datetime, timedelta

import pytest
from botocore.exceptions import ClientError
from conftest import BUCKET

from anp.resultado import (
    Resultado,
    chave_parquet,
    chave_rejeicao,
    listar_saidas,
    verificar_com_confirmacao,
    verificar_resultado,
)

DESDE = datetime(2026, 11, 15, 12, 0, tzinfo=UTC)
DEPOIS = DESDE + timedelta(minutes=2)
ANTES = DESDE - timedelta(hours=1)
PARQUET = chave_parquet(2026, 10)
REJEICAO = chave_rejeicao(2026, 10)
# Linhas de outros meses dentro do arquivo de out/2026: o P2 grava com o mesmo nome em
# outra partição.
VAZAMENTO_SETEMBRO = "processado/ano=2026/mes=09/precos-gasolina-etanol-2026-10.parquet"
VAZAMENTO_2025 = "processado/ano=2025/mes=12/precos-gasolina-etanol-2026-10.parquet"
# Arquivo de outro mês na mesma partição: não é saída do envio de out/2026.
OUTRO_ARQUIVO = "processado/ano=2026/mes=09/precos-gasolina-etanol-2026-09.parquet"


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


def test_listar_saidas_varias_particoes(s3):
    for chave in (PARQUET, VAZAMENTO_SETEMBRO, VAZAMENTO_2025, OUTRO_ARQUIVO):
        s3.gravar(chave, b"PAR1", DEPOIS)
    s3.gravar("rejeitado/precos-gasolina-etanol-2026-10.parquet", b"PAR1", DEPOIS)
    assert listar_saidas(s3, BUCKET, 2026, 10, DESDE) == [
        VAZAMENTO_2025,
        VAZAMENTO_SETEMBRO,
        PARQUET,
    ]


def test_listar_saidas_ignora_antigas(s3):
    s3.gravar(PARQUET, b"PAR1", DEPOIS)
    s3.gravar(VAZAMENTO_SETEMBRO, b"PAR1", ANTES)
    assert listar_saidas(s3, BUCKET, 2026, 10, DESDE) == [PARQUET]


def test_listar_saidas_pagina(s3):
    s3.tamanho_pagina = 2
    esperadas = [
        f"processado/ano=2026/mes={m:02d}/precos-gasolina-etanol-2026-10.parquet"
        for m in range(6, 11)
    ]
    for chave in [*esperadas, OUTRO_ARQUIVO, "processado/ano=2026/mes=08/x.parquet"]:
        s3.gravar(chave, b"PAR1", DEPOIS)
    assert listar_saidas(s3, BUCKET, 2026, 10, DESDE) == sorted(esperadas)
    assert s3.listagens == 4


def test_fora_do_mes_quando_so_existe_outra_particao(s3):
    s3.gravar(VAZAMENTO_SETEMBRO, b"PAR1", DEPOIS)
    assert verificar_resultado(s3, BUCKET, 2026, 10, DESDE) == Resultado(
        "fora_do_mes", None, (), (VAZAMENTO_SETEMBRO,)
    )


def test_pendente_quando_outra_particao_e_antiga(s3):
    s3.gravar(VAZAMENTO_SETEMBRO, b"PAR1", ANTES)
    assert verificar_resultado(s3, BUCKET, 2026, 10, DESDE) == Resultado("pendente")


def test_processado_mesmo_com_vazamento(s3):
    s3.gravar(PARQUET, b"PAR1", DEPOIS)
    s3.gravar(VAZAMENTO_SETEMBRO, b"PAR1", DEPOIS)
    resultado = verificar_resultado(s3, BUCKET, 2026, 10, DESDE)
    assert resultado.status == "processado"
    assert resultado.chave == PARQUET


def test_confirmacao_mantem_fora_do_mes(s3):
    s3.gravar(VAZAMENTO_SETEMBRO, b"PAR1", DEPOIS)
    esperas: list[float] = []
    resultado = verificar_com_confirmacao(s3, BUCKET, 2026, 10, DESDE, dormir=esperas.append)
    assert resultado == Resultado("fora_do_mes", None, (), (VAZAMENTO_SETEMBRO,))
    assert esperas == [10.0]


def test_confirmacao_fora_do_mes_transitorio_vira_processado(s3):
    s3.gravar(VAZAMENTO_SETEMBRO, b"PAR1", DEPOIS)

    def p2_termina_de_gravar(_: float) -> None:
        s3.gravar(PARQUET, b"PAR1", DEPOIS)

    resultado = verificar_com_confirmacao(s3, BUCKET, 2026, 10, DESDE, dormir=p2_termina_de_gravar)
    assert resultado == Resultado("processado", PARQUET)


def test_confirmacao_nao_espera_quando_pendente(s3):
    esperas: list[float] = []
    resultado = verificar_com_confirmacao(s3, BUCKET, 2026, 10, DESDE, dormir=esperas.append)
    assert resultado == Resultado("pendente")
    assert esperas == []
