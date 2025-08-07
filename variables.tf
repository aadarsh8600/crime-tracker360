#declare a region
variable "region" {
  default = "us-east-1"
}


variable "glue_role_arn" {
  description = "IAM role ARN to use for Glue Job"
  type        = string
}


#declare a bucket name for master data
#variable "bucket_name_for_masterdata" {
  #default = "silverbucket_for_masterdata2012310"
#}

#declare a bucket name for trasnformed data
#variable "bucket_name_for__transformeddata"{
  #default="goldbucket_for_transformedata2012310
#}

#declare a glue job name
variable "glue_job_name" {
  default = "glue-etl-job201"
}

#declare a crawler for dimesiomns table
#variable "glue_dimensions_crawler_name" {
  #default = "my-dimensions_crawler_name"
#}

#declare a crawler for facts table
variable "glue_facts_crawler_name"{
  default="my-facts-crawler"

declare a script path
variable "script_s3_path" {
  default = "s3://raw-master-transformed-factdim-grp-5/scripts/learning.py"
}
