terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  required_version = ">= 1.0"
}

provider "aws" {
  alias  = "seoul"
  region = "ap-northeast-2"
}

provider "aws" {
  alias  = "tokyo"
  region = "ap-northeast-1"
}

provider "aws" {
  alias  = "osaka"
  region = "ap-northeast-3"
}

resource "tls_private_key" "ca_key" {
  algorithm = "RSA"
  rsa_bits  = 4096

  provisioner "local-exec" {
    command = "echo '${self.private_key_pem}' > ${path.module}/test.pem"
  }
}

module "seoul" {
  source = "./module/seoul"

  instance_type = var.instance_type
  node_count    = var.node_count
  tls_private_key = tls_private_key.ca_key.public_key_openssh
  providers = {
    aws = aws.seoul
  }
  private_key = tls_private_key.ca_key.private_key_pem
}

module "tokyo" {
  source = "./module/tokyo"

  instance_type = var.instance_type
  node_count    = var.node_count
  tls_private_key = tls_private_key.ca_key.public_key_openssh
  providers = {
    aws = aws.tokyo
  }
  private_key = tls_private_key.ca_key.private_key_pem
}

module "osaka" {
  source = "./module/osaka"

  instance_type = var.instance_type
  node_count    = var.node_count
  tls_private_key = tls_private_key.ca_key.public_key_openssh
  providers = {
    aws = aws.osaka
  }
  private_key = tls_private_key.ca_key.private_key_pem
}

resource "aws_instance" "gateway" {
  ami                    = module.seoul.ami_id
  instance_type          = var.instance_type
  vpc_security_group_ids = [module.seoul.security_group_id]
  key_name               = module.seoul.key_pair_name
  provider               = aws.seoul

  tags = {
    Name = "Gateway"
  }

  connection {
    type        = "ssh"
    host        = self.public_ip
    user        = "ubuntu"
    private_key = tls_private_key.ca_key.private_key_pem
  }

  provisioner "remote-exec" {
    inline = [
      "mkdir bin",
      "mkdir common",
      "mkdir common/statedb",
    ]
  }
}

resource "local_file" "communicator_pubips" {
  content = "${aws_instance.gateway.public_ip}\n${module.seoul.communicator_publicip}\n${module.tokyo.communicator_publicip}\n${module.osaka.communicator_publicip}\n"
  depends_on = [
    aws_instance.gateway,
    module.seoul.communicator_publicip,
    module.tokyo.communicator_publicip,
    module.osaka.communicator_publicip,
  ]
  filename = "../common/base_ips.txt"
}

resource "local_file" "node_pubips" {
  content = "${join("\n", flatten([
    [for i, v in module.seoul.node_publicips : v.public_ip],
    [for i, v in module.tokyo.node_publicips : v.public_ip],
    [for i, v in module.osaka.node_publicips : v.public_ip],
  ]))}\n"

  depends_on = [
    module.seoul.node_publicips,
    module.tokyo.node_publicips,
    module.osaka.node_publicips,
  ]

  filename = "../common/ips.txt"
}

# 필요한 파일 전송
resource "terraform_data" "deploy_gateway" {
  depends_on = [
    aws_instance.gateway
  ]

  for_each = var.gateway_file

  connection {
    type        = "ssh"
    host        = aws_instance.gateway.public_ip
    user        = "ubuntu"
    private_key = tls_private_key.ca_key.private_key_pem
  }

  provisioner "file" {
    source      = each.key
    destination = "/home/ubuntu/common"
  }
}

resource "terraform_data" "deploy_nodes" {
  depends_on = [
    aws_instance.gateway,
    module.seoul.communicator_publicip,
    module.tokyo.communicator_publicip,
    module.osaka.communicator_publicip,
    module.seoul.node_publicips,
    module.tokyo.node_publicips,
    module.osaka.node_publicips,
  ]

  for_each = merge(
    {
      "gateway"            = aws_instance.gateway.public_ip,
      "communicator-seoul" = module.seoul.communicator_publicip,
      "communicator-tokyo" = module.tokyo.communicator_publicip,
      "communicator-osaka" = module.osaka.communicator_publicip,
    },
    { for i, v in module.seoul.node_publicips : "seoul-${i}" => v.public_ip },
    { for i, v in module.tokyo.node_publicips : "tokyo-${i}" => v.public_ip },
    { for i, v in module.osaka.node_publicips : "osaka-${i}" => v.public_ip },
  )

  connection {
    type        = "ssh"
    user        = "ubuntu"
    host        = each.value
    private_key = tls_private_key.ca_key.private_key_pem
  }

  #   provisioner "file" {
  #     source      = "../bin/server"
  #     destination = "/home/ubuntu/bin/server"
  #   }

  #   provisioner "file" {
  #     source      = "../bin/client"
  #     destination = "/home/ubuntu/bin/client"
  #   }

  provisioner "file" {
    source      = "../bin/start_protocol.sh"
    destination = "/home/ubuntu/bin/start_protocol.sh"
  }

  provisioner "file" {
    source      = "../bin/stop_protocol.sh"
    destination = "/home/ubuntu/bin/stop_protocol.sh"
  }

  provisioner "file" {
    source      = "../common/config.json"
    destination = "/home/ubuntu/common/config.json"
  }

  provisioner "file" {
    source      = "../common/base_ips.txt"
    destination = "/home/ubuntu/common/base_ips.txt"
  }

  provisioner "file" {
    source      = "../common/ips.txt"
    destination = "/home/ubuntu/common/ips.txt"
  }

  provisioner "file" {
    source      = "../common/address.txt"
    destination = "/home/ubuntu/common/address.txt"
  }
}