# Amazon Linux 2023 mais recente (parâmetro público da AWS).
data "aws_ssm_parameter" "al2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

data "aws_vpc" "padrao" {
  default = true
}

# Só AZs que oferecem o tipo de instância (nem toda AZ tem m7i-flex).
data "aws_ec2_instance_type_offerings" "tipo" {
  location_type = "availability-zone"

  filter {
    name   = "instance-type"
    values = [local.tipo_instancia]
  }
}

data "aws_subnets" "padrao" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.padrao.id]
  }

  filter {
    name   = "default-for-az"
    values = ["true"]
  }

  filter {
    name   = "availability-zone"
    values = data.aws_ec2_instance_type_offerings.tipo.locations
  }
}

# Nenhuma regra de entrada: o acesso é só pelo SSM. Saída liberada (ANP, S3, Docker Hub,
# GitHub) via IP público, sem NAT Gateway.
resource "aws_security_group" "airflow" {
  name        = "${local.prefix}-airflow"
  description = "Airflow do P4: sem ingresso, acesso via SSM"
  vpc_id      = data.aws_vpc.padrao.id

  egress {
    description = "Saida para a internet"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "airflow" {
  ami                         = data.aws_ssm_parameter.al2023.insecure_value
  instance_type               = local.tipo_instancia
  subnet_id                   = sort(data.aws_subnets.padrao.ids)[0]
  vpc_security_group_ids      = [aws_security_group.airflow.id]
  iam_instance_profile        = aws_iam_instance_profile.airflow.name
  associate_public_ip_address = true

  instance_initiated_shutdown_behavior = "stop"

  # IMDSv2 obrigatório; hop 2 para os containers alcançarem a role da instância.
  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = 30
    encrypted             = true
    delete_on_termination = true
  }

  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    regiao             = var.regiao
    repo_url           = var.repo_url
    bucket             = var.bucket_p2
    topico_alertas_arn = var.topico_alertas_arn_p2
    glue_database      = aws_glue_catalog_database.anp.name
    glue_tabela        = aws_glue_catalog_table.precos.name
    athena_workgroup   = aws_athena_workgroup.anp.name
    parametro_senha    = aws_ssm_parameter.senha_admin.name
    versao_compose     = var.versao_compose
    versao_buildx      = var.versao_buildx
  })

  tags = {
    Name = "${local.prefix}-airflow"
  }

  lifecycle {
    # Uma AMI nova publicada pela AWS não deve recriar a instância (e o banco do Airflow).
    ignore_changes = [ami]
  }
}
