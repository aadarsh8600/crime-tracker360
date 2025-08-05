resource "aws_s3_bucket" "raw_backup_bucket" {
  bucket = var.bucket_name
  acl    = "private"

  tags = {
    Name        = var.bucket_name
    Environment = "RawBackup"
  }
}
