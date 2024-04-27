resource "aws_sagemaker_notebook_instance" "notebook" {
  name          = "${var.prefix}-ml-resource"
  role_arn      = aws_iam_role.sagemaker_role.arn
  instance_type = "ml.t2.medium"
  #default_code_repository = aws_sagemaker_code_repository.repo.code_repository_name

  depends_on = [aws_iam_policy.sagemaker_policy] #, aws_sagemaker_code_repository.repo]

  tags = local.common_tags
}
