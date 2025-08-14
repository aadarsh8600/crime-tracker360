#declare a region
variable "region" {
  default = "us-east-1"
}


variable "glue_role_arn" {
  description = "IAM role ARN to use for Glue Job"
  type        = string
}



#declare a glue job name
variable "glue_job_name" {
  default = "glue-etl-job201"
}



#declare a crawler for facts table
variable "glue_crawler_name"{
  default="my_etl_crawler"
}

#declare a script path
variable "script_s3_path" {
  default = "s3://rawdatagroup5123123456/scripts/etlscript.py"
}
