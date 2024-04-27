# You can custom send files to s3

#resource "aws_s3_object" "bronze" {
#  bucket = "grc-lh-scripts"
#  key    = "jobs/spark_bronze.py"
#  source = "./jobs/spark_bronze.py"
#  etag   = filemd5("./jobs/spark_bronze.py")
#}
#
#resource "aws_s3_object" "silver" {
#  bucket = "grc-lh-scripts"
#  key    = "jobs/spark_silver.py"
#  source = "./jobs/spark_silver.py"
#  etag   = filemd5("./jobs/spark_silver.py")
#}
#
#resource "aws_s3_object" "gold" {
#  bucket = "grc-lh-scripts"
#  key    = "jobs/spark_gold.py"
#  source = "./jobs/spark_gold.py"
#  etag   = filemd5("./jobs/spark_gold.py")
#}
#
#resource "aws_s3_object" "analytics" {
#  bucket = "grc-lh-scripts"
#  key    = "jobs/spark_analytics.py"
#  source = "./jobs/spark_analytics.py"
#  etag   = filemd5("./jobs/spark_analytics.py")
#}
