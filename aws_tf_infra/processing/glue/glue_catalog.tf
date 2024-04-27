resource "aws_glue_catalog_database" "bronze" {
  name = "${var.prefix}_iceberg_bronze"
  tags = local.common_tags
}

resource "aws_glue_catalog_database" "silver" {
  name = "${var.prefix}_iceberg_silver"
  tags = local.common_tags
}

resource "aws_glue_catalog_database" "gold" {
  name = "${var.prefix}_iceberg_gold"
  tags = local.common_tags
}

resource "aws_glue_catalog_database" "analytics" {
  name = "${var.prefix}_iceberg_analytics"
  tags = local.common_tags
}
