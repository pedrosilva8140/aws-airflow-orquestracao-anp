variable "regiao" {
  description = "Região AWS (a SCP da conta nega quase tudo fora de us-east-2)."
  type        = string
  default     = "us-east-2"
}

variable "projeto" {
  description = "Nome do projeto: prefixo dos recursos e tag Projeto."
  type        = string
  default     = "aws-airflow-orquestracao-anp"
}

variable "bucket_p2" {
  description = "Output `bucket` do P2 (aws-pipeline-eventos-anp)."
  type        = string
}

variable "topico_alertas_arn_p2" {
  description = "Output `topico_alertas_arn` do P2: tópico SNS usado também para os alertas do Airflow."
  type        = string

  validation {
    condition     = can(regex("^arn:aws:sns:", var.topico_alertas_arn_p2))
    error_message = "topico_alertas_arn_p2 deve ser um ARN de tópico SNS."
  }
}

variable "repo_url" {
  description = "Repositório público clonado pela instância."
  type        = string
  default     = "https://github.com/pedrosilva8140/aws-airflow-orquestracao-anp.git"
}

variable "glue_database" {
  description = "Database do Glue com a tabela do P2."
  type        = string
  default     = "anp_combustiveis"
}

variable "glue_tabela" {
  description = "Tabela externa sobre processado/ do P2."
  type        = string
  default     = "precos_gasolina_etanol"
}

variable "versao_compose" {
  description = "Versão do plugin Docker Compose instalado pelo user_data (o AL2023 não o empacota)."
  type        = string
  default     = "5.8.0"
}

variable "versao_buildx" {
  description = "Versão do plugin buildx (exigido pelo `docker compose build`)."
  type        = string
  default     = "0.39.1"
}
