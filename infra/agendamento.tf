# Trava de custo: a instância é desligada todo dia às 23:00 (horário de Brasília).
# Ela só é ligada à mão, pelo scripts/abrir_airflow.ps1.
resource "aws_scheduler_schedule" "desligar" {
  name        = "${local.prefix}-desligar"
  description = "Para a EC2 do Airflow todo dia as 23:00"

  schedule_expression          = "cron(0 23 * * ? *)"
  schedule_expression_timezone = "America/Sao_Paulo"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = "arn:aws:scheduler:::aws-sdk:ec2:stopInstances"
    role_arn = aws_iam_role.desligamento.arn
    input    = jsonencode({ InstanceIds = [aws_instance.airflow.id] })

    retry_policy {
      maximum_retry_attempts = 3
    }
  }
}
