from pathlib import Path

import pytest

from anp.descoberta import MesAmbiguo, MesNaoPublicado, encontrar_url_mes, extrair_links

FIXTURE = Path(__file__).parent / "fixtures" / "pagina_anp.html"
PREFIXO = "/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsan"


@pytest.fixture(scope="module")
def html() -> str:
    return FIXTURE.read_text(encoding="utf-8")


def test_extrair_links_le_hrefs_das_ancoras(html):
    links = extrair_links(html)
    assert len(links) == 7
    assert f"{PREFIXO}/2026/10-dados-abertos-precos-glp.csv" in links


def test_outubro_2026(html):
    assert encontrar_url_mes(html, 2026, 10).endswith("/2026/10-gasolina-etanol.csv")


def test_setembro_2026_com_ano_no_nome(html):
    url = encontrar_url_mes(html, 2026, 9)
    assert url.endswith("/2026/09-dados-abertos-precos-2026-09-gasolina-etanol.csv")


def test_abril_2026_sem_extensao(html):
    url = encontrar_url_mes(html, 2026, 4)
    assert url.endswith("/2026/04-dados-abertos-precos-gasolina-etanol")


def test_fevereiro_2026_com_erro_de_digitacao(html):
    url = encontrar_url_mes(html, 2026, 2)
    assert url.endswith("/2026/02-cados-abertos-preco-gasolina-etanol.csv")


def test_dezembro_2025_padrao_antigo(html):
    assert encontrar_url_mes(html, 2025, 12).endswith("/2025/precos-gasolina-etanol-12.csv")


def test_ignora_glp(html):
    assert "glp" not in encontrar_url_mes(html, 2026, 10)


def test_mes_nao_publicado(html):
    with pytest.raises(MesNaoPublicado, match="2026-11"):
        encontrar_url_mes(html, 2026, 11)


def test_mes_ambiguo():
    html = (
        f'<a href="{PREFIXO}/2026/10-gasolina-etanol.csv">a</a>'
        f'<a href="{PREFIXO}/2026/10-dados-abertos-precos-gasolina-etanol.csv">b</a>'
    )
    with pytest.raises(MesAmbiguo, match="2026-10"):
        encontrar_url_mes(html, 2026, 10)


def test_mesmo_link_repetido_nao_e_ambiguo():
    href = f"{PREFIXO}/2026/10-gasolina-etanol.csv"
    html = f'<a href="{href}">a</a><a href="https://www.gov.br{href}">b</a>'
    assert encontrar_url_mes(html, 2026, 10) == f"https://www.gov.br{href}"


def test_url_relativa_vira_absoluta(html):
    url = encontrar_url_mes(html, 2025, 12)
    assert url.startswith("https://www.gov.br/")
