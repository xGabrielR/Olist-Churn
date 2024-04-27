resource "aws_iam_role" "dms_s3_role" {
  name        = "dms-s3-role"
  description = "Role used to migrate data from S3 via DMS"

  assume_role_policy = jsonencode(
    {
      Version = "2012-10-17"
      Statement = [
        {
          Sid    = "DMSAssume"
          Action = "sts:AssumeRole"
          Effect = "Allow"
          Principal = {
            Service = "dms.${data.aws_partition.current.dns_suffix}"
          }
        },
      ]
    }
  )

  inline_policy {
    name = "dms-s3-role"
    policy = jsonencode(
      {
        Version = "2012-10-17"
        Statement = [
          {
            Sid      = "DMSS3"
            Action   = ["s3:*"]
            Effect   = "Allow"
            Resource = "*"
          }
        ]
      }
    )
  }
}

resource "aws_iam_role" "dms-vpc-role" {
  assume_role_policy = data.aws_iam_policy_document.dms_assume_role.json
  name               = "dms-vpc-role"
}

resource "aws_iam_role_policy_attachment" "dms-vpc-role-AmazonDMSVPCManagementRole" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonDMSVPCManagementRole"
  role       = aws_iam_role.dms-vpc-role.name
}
