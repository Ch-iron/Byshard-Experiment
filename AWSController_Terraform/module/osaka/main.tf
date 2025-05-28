terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

resource "aws_key_pair" "generated_key_osaka" {
  key_name   = "test-key"
  public_key = var.tls_private_key
}

data "aws_ami" "osaka" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_security_group" "sg_osaka" {
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1" # all protocols
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "sg-Byshard"
  }
}

resource "aws_instance" "communicator_osaka" {
  ami                    = data.aws_ami.osaka.id
  instance_type          = var.instance_type
  vpc_security_group_ids = [aws_security_group.sg_osaka.id]
  key_name               = aws_key_pair.generated_key_osaka.key_name
  tags = {
    Name = "Communicator"
  }

  connection {
    type        = "ssh"
    host        = self.public_ip
    user        = "ubuntu"
    private_key = var.private_key
  }

  provisioner "remote-exec" {
    inline = [
      "mkdir bin",
      "mkdir common",
      "mkdir common/statedb"
    ]
  }
}

resource "aws_instance" "node_osaka" {
  count                  = var.node_count
  ami                    = data.aws_ami.osaka.id
  instance_type          = var.instance_type
  vpc_security_group_ids = [aws_security_group.sg_osaka.id]
  key_name               = aws_key_pair.generated_key_osaka.key_name

  tags = {
    Name = "Consensus-Node-${count.index + 1}"
  }

  connection {
    type        = "ssh"
    host        = self.public_ip
    user        = "ubuntu"
    private_key = var.private_key
  }

  provisioner "remote-exec" {
    inline = [
      "mkdir bin",
      "mkdir common",
      "mkdir common/statedb"
    ]
  }
}