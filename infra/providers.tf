terraform {
  required_version = ">= 1.17.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # State local (fora do git, ver .gitignore). Tudo é destruído no fim do projeto.
}

provider "aws" {
  region = var.regiao

  default_tags {
    tags = {
      Projeto    = var.projeto
      Gerenciado = "terraform"
    }
  }
}

data "aws_caller_identity" "atual" {}

locals {
  conta  = data.aws_caller_identity.atual.account_id
  prefix = var.projeto

  # A instância é fixa: o plano gratuito só cobre esta família (regra de custo do projeto).
  tipo_instancia = "m7i-flex.large"

  arn_bucket_p2      = "arn:aws:s3:::${var.bucket_p2}"
  prefixo_resultados = "athena-resultados/"
  nome_parametro     = "/${var.projeto}/airflow-admin-senha"
}
