# You can add a extra repository code for sagemaker instance
# with keys on aws vault

#resource "aws_sagemaker_code_repository" "repo" {
#  code_repository_name = "${var.prefix}-${var.environment}-git-repo"
#
#  git_config {
#    repository_url = var.repository_url
#    secret_arn     = aws_secretsmanager_secret.git.arn
#  }
#
#  depends_on = [aws_secretsmanager_secret_version.git_credentials]
#
#  tags = local.common_tags
#}
