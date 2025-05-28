output "key_pair_name" {
  description = "The name of the key pair created in the Seoul region"
  value       = aws_key_pair.generated_key_seoul.key_name
}

output "ami_id" {
  description = "The AMI ID used for the instances in the Seoul region"
  value       = data.aws_ami.seoul.id
}

output "security_group_id" {
  description = "The security group ID for the instances in the Seoul region"
  value       = aws_security_group.sg_seoul.id
}

output "communicator_publicip" {
  description = "The public IP address of the communicator instance in the Seoul region"
  value       = aws_instance.communicator_seoul.public_ip
}

output "node_publicips" {
  description = "A map of public IP addresses for the consensus nodes in the Seoul region"
  value = aws_instance.node_seoul
}