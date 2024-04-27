resource "tls_private_key" "rsa" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "aws_key_pair" "key" {
  key_name   = "${var.prefix}-lh-ec2-key"
  public_key = tls_private_key.rsa.public_key_openssh
  tags       = local.common_tags
}

resource "local_file" "key_pairs" {
  content  = tls_private_key.rsa.private_key_pem
  filename = "${var.prefix}-lh-ec2-key.pem"
}
