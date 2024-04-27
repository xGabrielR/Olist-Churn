resource "aws_iam_role" "sagemaker_role" {
  name               = "GrcSagemakerRole"
  path               = "/"
  description        = "Aws sagemaker role"
  assume_role_policy = file("./permissions/sagemaker_role.json")
}

resource "aws_iam_policy" "sagemaker_policy" {
  name        = "GrcSagemakerPolicy"
  path        = "/"
  description = "Aws sagemaker policy"
  policy      = file("./permissions/sagemaker_policy.json")
}

resource "aws_iam_role_policy_attachment" "attach" {
  role       = aws_iam_role.sagemaker_role.name
  policy_arn = aws_iam_policy.sagemaker_policy.arn
}
