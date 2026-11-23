"""Portões de qualidade do Parquet gerado pelo P2, antes da publicação no Athena.

`checar` devolve uma lista de falhas legíveis: vazia significa aprovado. Qualquer
falha bloqueia a publicação da partição.
"""

import calendar
import io
from collections.abc import Sequence
from datetime import date

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from anp.resultado import chave_parquet

LINHAS_MINIMAS = 10_000
PRECO_MIN = 2.0
PRECO_MAX = 15.0
UFS_MINIMAS = 27
PRODUTOS_OBRIGATORIOS = frozenset({"GASOLINA", "ETANOL"})
UFS_BRASIL = frozenset(
    {
        "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA",
        "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO",
    }
)  # fmt: skip
EXEMPLOS = 5


def _distintos(tabela: pa.Table, coluna: str) -> set:
    return set(pc.unique(tabela.column(coluna)).drop_null().to_pylist())


def _checar_linhas(tabela: pa.Table, linhas_minimas: int) -> list[str]:
    if tabela.num_rows < linhas_minimas:
        return [f"poucas linhas: {tabela.num_rows} (mínimo {linhas_minimas})"]
    return []


def _checar_precos(tabela: pa.Table) -> list[str]:
    precos = tabela.column("valor_venda")
    fora = pc.fill_null(pc.or_(pc.less(precos, PRECO_MIN), pc.greater(precos, PRECO_MAX)), False)
    quantidade = pc.sum(fora).as_py() or 0
    if not quantidade:
        return []
    exemplos = sorted(set(precos.filter(fora).to_pylist()))[:EXEMPLOS]
    return [
        f"valor_venda fora da faixa [{PRECO_MIN}, {PRECO_MAX}]: "
        f"{quantidade} linha(s), exemplos: {exemplos}"
    ]


def _checar_datas(tabela: pa.Table, ano: int, mes: int) -> list[str]:
    datas = tabela.column("data_coleta")
    primeiro = date(ano, mes, 1)
    ultimo = date(ano, mes, calendar.monthrange(ano, mes)[1])
    fora = pc.fill_null(pc.or_(pc.less(datas, primeiro), pc.greater(datas, ultimo)), False)
    quantidade = pc.sum(fora).as_py() or 0
    if not quantidade:
        return []
    extremos = pc.min_max(datas.filter(fora)).as_py()
    return [
        f"data_coleta fora de {ano}-{mes:02d}: {quantidade} linha(s), "
        f"entre {extremos['min']} e {extremos['max']}"
    ]


def _checar_ufs(tabela: pa.Table) -> list[str]:
    presentes = _distintos(tabela, "uf")
    if len(presentes) >= UFS_MINIMAS:
        return []
    faltando = ", ".join(sorted(UFS_BRASIL - presentes)) or "-"
    return [f"apenas {len(presentes)} UF(s) (mínimo {UFS_MINIMAS}); faltando: {faltando}"]


def _checar_produtos(tabela: pa.Table) -> list[str]:
    faltando = PRODUTOS_OBRIGATORIOS - _distintos(tabela, "produto")
    if not faltando:
        return []
    return [f"produto(s) obrigatório(s) ausente(s): {', '.join(sorted(faltando))}"]


def _checar_vazamento(saidas: Sequence[str], ano: int, mes: int) -> list[str]:
    esperada = chave_parquet(ano, mes)
    outras = sorted(chave for chave in saidas if chave != esperada)
    if not outras:
        return []
    return [f"vazamento para outra partição: {', '.join(outras)}"]


def checar(
    tabela: pa.Table,
    ano: int,
    mes: int,
    saidas: Sequence[str],
    linhas_minimas: int = LINHAS_MINIMAS,
) -> list[str]:
    """Roda todos os portões e devolve as falhas (lista vazia = aprovado).

    `saidas` é o resultado de `listar_saidas`: qualquer chave fora da partição do mês
    reprova, porque significa linhas do arquivo publicadas em outra partição.
    """
    return [
        *_checar_linhas(tabela, linhas_minimas),
        *_checar_precos(tabela),
        *_checar_datas(tabela, ano, mes),
        *_checar_ufs(tabela),
        *_checar_produtos(tabela),
        *_checar_vazamento(saidas, ano, mes),
    ]


def ler_parquet(s3, bucket: str, chave: str) -> pa.Table:
    corpo = s3.get_object(Bucket=bucket, Key=chave)["Body"].read()
    return pq.read_table(io.BytesIO(corpo))
