output "instance_id" {
  description = "ID da EC2 do Airflow (usado pelos scripts em scripts/)."
  value       = aws_instance.airflow.id
}

output "comando_port_forward" {
  description = "Abre a interface do Airflow em http://localhost:8080 (requer o Session Manager plugin)."
  value       = "aws ssm start-session --region ${var.regiao} --target ${aws_instance.airflow.id} --document-name AWS-StartPortForwardingSession --parameters portNumber=8080,localPortNumber=8080"
}

output "parametro_senha_admin" {
  description = "Nome do parâmetro SSM com a senha do admin (o valor não é exposto)."
  value       = aws_ssm_parameter.senha_admin.name
}

output "athena_workgroup" {
  description = "Workgroup do Athena (limite de 100 MB por consulta)."
  value       = aws_athena_workgroup.anp.name
}
