data "aws_ami" "ubuntu" {
  most_recent = true

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  owners = ["099720109477"] # Canonical
}

resource "aws_instance" "machine" {
  instance_type = var.instance_type
  ami           = data.aws_ami.ubuntu.id

  subnet_id              = ""   # You can use the same vpc of sagemaker and dms
  vpc_security_group_ids = [""] # You can use the same vpc of sagemaker and dms

  key_name   = "${var.prefix}-lh-ec2-key"
  depends_on = [local_file.key_pairs]

  tags = {
    Name      = "${var.prefix}-lh-ec2"
    Project   = "grc-olist-churn"
    Terraform = true
  }
}

output "instance_ip" {
  description = "The public ip for ssh access"
  value       = aws_instance.machine.public_ip
}
