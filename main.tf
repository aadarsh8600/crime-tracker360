resource "aws_s3_bucket" "masterdata" {
  bucket = var.bucket_name_for_masterdata
}

resource "aws_s3_bucket" "trasnformeddata"{
  bucket=var.bucket_name_for__transformeddata
}

resource "aws_glue_catalog_database" "etl_db" {
  name = "crime_db123"
}

locals {
  glue_role_arn = var.glue_role_arn
}

resource "aws_glue_job" "etl_job" {
  name     = var.glue_job_name
  role_arn = local.glue_role_arn

  command {
    name            = "glueetl"
    script_location = var.script_s3_path
    python_version  = "3"
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
}

resource "aws_glue_crawler" "facts_crawler" {
  name          = var.glue_facts_crawler_name
  role          = local.glue_role_arn
  database_name = aws_glue_catalog_database.etl_db.name

  s3_target {
    path = "s3://${aws_s3_bucket.etl_bucket.bucket}/facts_data/"
 }

  depends_on = [aws_glue_job.etl_job]
}
resource "aws_glue_crawler" "dimension_crawler"{
  name=var.glue_dimensions_crawler_name
  role=local.glue_role_arn
  database_name=aws_glue_catalog_database.etl_db_name
  s3_target {
    path="s3://${aws_s3_bucket.etl_bucket.bucket}/dimensions_data/"
}

  depends_on=[aws_glue_job.etl_job]
}
