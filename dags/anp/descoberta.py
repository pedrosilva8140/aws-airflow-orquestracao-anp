"""Descoberta da URL do arquivo mensal de gasolina e etanol na página da ANP.

Os nomes de arquivo da ANP são inconsistentes (erro de digitação "cados", arquivo
sem extensão, mês no início ou no fim do nome). Por isso a URL nunca é montada por
padrão: ela é encontrada entre os links da página, pelo ano no caminho e pelo mês
no nome do arquivo.
"""

import re
from html.parser import HTMLParser
from urllib.parse import unquote, urljoin, urlparse


class MesNaoPublicado(Exception):
    """A página ainda não tem o arquivo do mês pedido."""


class MesAmbiguo(Exception):
    """A página tem mais de um arquivo candidato para o mês pedido."""


class _ColetorDeLinks(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        for nome, valor in attrs:
            if nome == "href" and valor:
                self.links.append(valor.strip())


def extrair_links(html: str) -> list[str]:
    """Devolve os href de todas as âncoras do HTML, na ordem em que aparecem."""
    coletor = _ColetorDeLinks()
    coletor.feed(html)
    coletor.close()
    return coletor.links


def _mes_do_nome(nome_arquivo: str) -> int | None:
    """Primeiro token de 2 dígitos entre 01 e 12, delimitado por início, fim, '-', '_' ou '.'.

    Tokens de 4 dígitos (o ano) não têm 2 dígitos e por isso são desconsiderados.
    """
    for token in re.split(r"[-_.]", nome_arquivo):
        if len(token) == 2 and token.isdigit() and 1 <= int(token) <= 12:
            return int(token)
    return None


def encontrar_url_mes(html: str, ano: int, mes: int, base: str = "https://www.gov.br") -> str:
    """Devolve a URL absoluta do arquivo de gasolina e etanol de `ano`/`mes`.

    Levanta `MesNaoPublicado` se não houver candidato e `MesAmbiguo` se houver mais de um.
    """
    referencia = f"{ano}-{mes:02d}"
    candidatos: list[str] = []
    for href in extrair_links(html):
        url = urljoin(base + "/", href)
        caminho = unquote(urlparse(url).path)
        if f"/dsan/{ano}/" not in caminho:
            continue
        nome = caminho.rsplit("/", 1)[-1].lower()
        if "gasolina" not in nome or "etanol" not in nome:
            continue
        if _mes_do_nome(nome) == mes and url not in candidatos:
            candidatos.append(url)

    if not candidatos:
        raise MesNaoPublicado(
            f"A ANP ainda não publicou o arquivo de gasolina e etanol de {referencia}."
        )
    if len(candidatos) > 1:
        raise MesAmbiguo(f"Mais de um arquivo de gasolina e etanol para {referencia}: {candidatos}")
    return candidatos[0]
