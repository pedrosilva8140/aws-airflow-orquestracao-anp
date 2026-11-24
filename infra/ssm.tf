# Senha do admin da interface do Airflow. Sem caracteres especiais para não precisar de
# escape no .env. SecureString com a chave gerenciada pela AWS (aws/ssm), sem KMS próprio.
resource "random_password" "senha_admin" {
  length  = 32
  special = false
}

resource "aws_ssm_parameter" "senha_admin" {
  name        = local.nome_parametro
  description = "Senha do usuário admin do Airflow (P4)"
  type        = "SecureString"
  value       = random_password.senha_admin.result
}
