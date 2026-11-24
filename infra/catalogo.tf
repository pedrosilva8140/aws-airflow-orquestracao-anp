resource "aws_glue_catalog_database" "anp" {
  name        = var.glue_database
  description = "Preços de combustíveis da ANP processados pelo P2"
}

# Tabela externa sobre processado/ do P2. As partições (ano, mes) são publicadas pelo
# DAG só depois dos portões de qualidade (ALTER TABLE ... ADD IF NOT EXISTS PARTITION).
resource "aws_glue_catalog_table" "precos" {
  name          = var.glue_tabela
  database_name = aws_glue_catalog_database.anp.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL       = "TRUE"
    classification = "parquet"
  }

  partition_keys {
    name = "ano"
    type = "string"
  }

  partition_keys {
    name = "mes"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${var.bucket_p2}/processado/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    # Mesmo esquema do Parquet do P2 (ESQUEMA_PARQUET em transformacao.py): 16 colunas.
    dynamic "columns" {
      for_each = [
        { nome = "regiao", tipo = "string" },
        { nome = "uf", tipo = "string" },
        { nome = "municipio", tipo = "string" },
        { nome = "revenda", tipo = "string" },
        { nome = "cnpj", tipo = "string" },
        { nome = "nome_rua", tipo = "string" },
        { nome = "numero_rua", tipo = "string" },
        { nome = "complemento", tipo = "string" },
        { nome = "bairro", tipo = "string" },
        { nome = "cep", tipo = "string" },
        { nome = "produto", tipo = "string" },
        { nome = "data_coleta", tipo = "date" },
        { nome = "valor_venda", tipo = "double" },
        { nome = "valor_compra", tipo = "double" },
        { nome = "unidade_medida", tipo = "string" },
        { nome = "bandeira", tipo = "string" },
      ]

      content {
        name = columns.value.nome
        type = columns.value.tipo
      }
    }
  }
}

# Trava de custo: nenhuma consulta passa de 100 MB lidos. Resultados no bucket do P2.
resource "aws_athena_workgroup" "anp" {
  name          = local.prefix
  description   = "Consultas do P4 (DAG e demonstrações)"
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = false
    bytes_scanned_cutoff_per_query     = 104857600

    result_configuration {
      output_location = "s3://${var.bucket_p2}/${local.prefixo_resultados}"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
