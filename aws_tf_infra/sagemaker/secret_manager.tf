#resource "aws_secretsmanager_secret" "git" {
#  name        = "${var.prefix}-lh-${var.environment}-git"
#  description = "Created By: ..., git credentials for sagemaker machine: ...."
#
#  recovery_window_in_days = 0
#
#  tags = local.common_tags
#}
#
#resource "aws_secretsmanager_secret_version" "git_credentials" {
#  secret_id     = aws_secretsmanager_secret.git.id
#  secret_string = jsonencode(var.repo_secret_json)
#}
