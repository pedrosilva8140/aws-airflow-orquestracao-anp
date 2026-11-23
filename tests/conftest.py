from datetime import UTC, datetime
from typing import BinaryIO

import pytest
from botocore.exceptions import ClientError

BUCKET = "bucket-teste"


def erro_aws(codigo: str, operacao: str) -> ClientError:
    return ClientError({"Error": {"Code": codigo, "Message": codigo}}, operacao)


class S3Falso:
    """Imita só as chamadas do cliente S3 que o pacote `anp` usa, guardando tudo em memória.

    `agora` é o horário gravado como `LastModified` nas próximas escritas.
    """

    def __init__(self, agora: datetime | None = None) -> None:
        self.agora = agora or datetime(2026, 11, 15, 12, 0, tzinfo=UTC)
        self.objetos: dict[tuple[str, str], bytes] = {}
        self.modificados: dict[tuple[str, str], datetime] = {}

    def upload_fileobj(self, Fileobj: BinaryIO, Bucket: str, Key: str, **_: object) -> None:
        self.objetos[(Bucket, Key)] = Fileobj.read()
        self.modificados[(Bucket, Key)] = self.agora

    def head_object(self, Bucket: str, Key: str) -> dict:
        if (Bucket, Key) not in self.objetos:
            raise erro_aws("404", "HeadObject")
        return {
            "ContentLength": len(self.objetos[(Bucket, Key)]),
            "LastModified": self.modificados[(Bucket, Key)],
        }

    def chaves(self) -> list[str]:
        return sorted(chave for _, chave in self.objetos)


@pytest.fixture
def s3() -> S3Falso:
    return S3Falso()
