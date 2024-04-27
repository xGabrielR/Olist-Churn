resource "aws_iam_role" "EmrDefaultRole" {
  name               = "EmrDefaultRole"
  path               = "/"
  description        = "Default role for EMR"
  assume_role_policy = file("./permissions/EmrDefaultRole.json")
}

resource "aws_iam_policy" "iam_emr_service_policy" {
  name        = "iam_emr_service_policy"
  path        = "/"
  description = "Policies for EMR Default Role"
  policy      = file("./permissions/iam_emr_service_policy.json")
}

resource "aws_iam_role_policy_attachment" "emr_role_attach" {
  role       = aws_iam_role.EmrDefaultRole.name
  policy_arn = aws_iam_policy.iam_emr_service_policy.arn
}

resource "aws_iam_role" "EmrEc2DefaultRole" {
  name               = "EmrEc2DefaultRole"
  path               = "/"
  description        = "Default role for EMR EC2"
  assume_role_policy = file("./permissions/EmrEc2DefaultRole.json")
}

resource "aws_iam_policy" "emr_profile" {
  name        = "emr_profile"
  path        = "/"
  description = "Policies for EMR EC2 Default Role"
  policy      = file("./permissions/emr_profile.json")
}

resource "aws_iam_role_policy_attachment" "emr_ec2_role_attach" {
  role       = aws_iam_role.EmrEc2DefaultRole.name
  policy_arn = aws_iam_policy.emr_profile.arn
}

resource "aws_iam_instance_profile" "emr_profile" {
  name = "EmrEc2DefaultRole_profile"
  role = aws_iam_role.EmrEc2DefaultRole.name
}
