import io
from datetime import UTC, date, datetime

import pyarrow as pa
import pyarrow.parquet as pq
from conftest import BUCKET

from anp.qualidade import UFS_BRASIL, checar, ler_parquet
from anp.resultado import chave_parquet

ESPERADA = chave_parquet(2026, 10)
VAZAMENTO = "processado/ano=2026/mes=09/precos-gasolina-etanol-2026-10.parquet"
PRODUTOS = ["GASOLINA", "GASOLINA ADITIVADA", "ETANOL"]


def tabela_boa(linhas: int = 10_800, ufs: list[str] | None = None) -> pa.Table:
    """Tabela que passa em todos os portões: todas as UFs, produtos e dias de out/2026."""
    ufs = ufs or sorted(UFS_BRASIL)
    return pa.table(
        {
            "uf": [ufs[i % len(ufs)] for i in range(linhas)],
            "produto": [PRODUTOS[i % len(PRODUTOS)] for i in range(linhas)],
            "data_coleta": [date(2026, 10, 1 + i % 31) for i in range(linhas)],
            "valor_venda": [4.0 + (i % 300) / 100 for i in range(linhas)],
        }
    )


def substituir(tabela: pa.Table, coluna: str, valores: dict[int, object]) -> pa.Table:
    lista = tabela.column(coluna).to_pylist()
    for indice, valor in valores.items():
        lista[indice] = valor
    posicao = tabela.schema.get_field_index(coluna)
    return tabela.set_column(posicao, coluna, pa.array(lista, tabela.schema.field(coluna).type))


def test_aprova_tabela_boa():
    assert checar(tabela_boa(), 2026, 10, [ESPERADA]) == []


def test_poucas_linhas():
    falhas = checar(tabela_boa(linhas=500), 2026, 10, [ESPERADA])
    assert len(falhas) == 1
    assert "500" in falhas[0] and "10000" in falhas[0]


def test_preco_fora_da_faixa():
    tabela = substituir(tabela_boa(), "valor_venda", {0: 0.5, 1: 40.0})
    falhas = checar(tabela, 2026, 10, [ESPERADA])
    assert len(falhas) == 1
    assert "0.5" in falhas[0] and "40.0" in falhas[0]


def test_data_fora_do_mes():
    tabela = substituir(tabela_boa(), "data_coleta", {0: date(2026, 9, 30)})
    falhas = checar(tabela, 2026, 10, [ESPERADA])
    assert len(falhas) == 1
    assert "2026-09-30" in falhas[0]


def test_uf_faltando():
    ufs = sorted(UFS_BRASIL - {"AC"})
    falhas = checar(tabela_boa(ufs=ufs), 2026, 10, [ESPERADA])
    assert len(falhas) == 1
    assert "26" in falhas[0] and "AC" in falhas[0]


def test_produto_obrigatorio_faltando():
    tabela = tabela_boa()
    tabela = tabela.set_column(
        tabela.schema.get_field_index("produto"),
        "produto",
        pa.array(["GASOLINA"] * tabela.num_rows),
    )
    falhas = checar(tabela, 2026, 10, [ESPERADA])
    assert len(falhas) == 1
    assert "ETANOL" in falhas[0]


def test_linhas_minimas_sobrescrito():
    falhas = checar(tabela_boa(), 2026, 10, [ESPERADA], linhas_minimas=10_000_000)
    assert len(falhas) == 1
    assert "10000000" in falhas[0]


def test_vazamento_reprova():
    falhas = checar(tabela_boa(), 2026, 10, [VAZAMENTO, ESPERADA])
    assert falhas == [f"vazamento para outra partição: {VAZAMENTO}"]


def test_ler_parquet(s3):
    buffer = io.BytesIO()
    pq.write_table(tabela_boa(linhas=10), buffer)
    s3.gravar(ESPERADA, buffer.getvalue(), datetime(2026, 11, 15, tzinfo=UTC))
    assert ler_parquet(s3, BUCKET, ESPERADA).equals(tabela_boa(linhas=10))


def test_ufs_brasil_tem_27():
    assert len(UFS_BRASIL) == 27
