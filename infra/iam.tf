# ---------------------------------------------------------------------------
# Role da instância: SSM + só o que o DAG usa no P2, no Glue e no Athena.
# ---------------------------------------------------------------------------
data "aws_iam_policy_document" "confianca_ec2" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "airflow" {
  name               = "${local.prefix}-instancia"
  assume_role_policy = data.aws_iam_policy_document.confianca_ec2.json
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.airflow.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

data "aws_iam_policy_document" "airflow" {
  statement {
    sid       = "EnviarParaP2"
    actions   = ["s3:PutObject"]
    resources = ["${local.arn_bucket_p2}/entrada/*"]
  }

  statement {
    sid     = "LerResultadoDoP2"
    actions = ["s3:GetObject"]
    resources = [
      "${local.arn_bucket_p2}/processado/*",
      "${local.arn_bucket_p2}/rejeitado/*",
    ]
  }

  # Sem condição de prefixo: com s3:ListBucket, um HeadObject em chave inexistente
  # devolve 404 (e não 403), que o sensor trata como "pendente". Só lista chaves.
  statement {
    sid       = "ListarBucketDoP2"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [local.arn_bucket_p2]
  }

  statement {
    sid = "ResultadosDoAthena"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${local.arn_bucket_p2}/${local.prefixo_resultados}*"]
  }

  statement {
    sid = "PublicarParticaoNoGlue"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:CreatePartition",
      "glue:BatchCreatePartition",
    ]
    resources = [
      "arn:aws:glue:${var.regiao}:${local.conta}:catalog",
      aws_glue_catalog_database.anp.arn,
      aws_glue_catalog_table.precos.arn,
    ]
  }

  statement {
    sid = "ConsultasNoWorkgroup"
    actions = [
      "athena:StartQueryExecution",
      "athena:GetQueryExecution",
      "athena:StopQueryExecution",
      "athena:GetWorkGroup",
    ]
    resources = [aws_athena_workgroup.anp.arn]
  }

  statement {
    sid       = "AlertasNoSnsDoP2"
    actions   = ["sns:Publish"]
    resources = [var.topico_alertas_arn_p2]
  }

  statement {
    sid       = "SenhaDoAdmin"
    actions   = ["ssm:GetParameter"]
    resources = [aws_ssm_parameter.senha_admin.arn]
  }
}

resource "aws_iam_role_policy" "airflow" {
  name   = "${local.prefix}-dag"
  role   = aws_iam_role.airflow.id
  policy = data.aws_iam_policy_document.airflow.json
}

resource "aws_iam_instance_profile" "airflow" {
  name = "${local.prefix}-instancia"
  role = aws_iam_role.airflow.name
}

# ---------------------------------------------------------------------------
# Role do EventBridge Scheduler: só parar esta instância.
# ---------------------------------------------------------------------------
data "aws_iam_policy_document" "confianca_scheduler" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.conta]
    }
  }
}

resource "aws_iam_role" "desligamento" {
  name               = "${local.prefix}-desligamento"
  assume_role_policy = data.aws_iam_policy_document.confianca_scheduler.json
}

data "aws_iam_policy_document" "desligamento" {
  statement {
    actions   = ["ec2:StopInstances"]
    resources = [aws_instance.airflow.arn]
  }
}

resource "aws_iam_role_policy" "desligamento" {
  name   = "${local.prefix}-parar-instancia"
  role   = aws_iam_role.desligamento.id
  policy = data.aws_iam_policy_document.desligamento.json
}
