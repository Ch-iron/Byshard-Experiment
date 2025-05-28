output "communicator_publicip" {
  description = "The public IP address of the communicator instance in the Osaka region"
  value       = aws_instance.communicator_osaka.public_ip
}

output "node_publicips" {
  description = "A map of public IP addresses for the consensus nodes in the Osaka region"
  value = aws_instance.node_osaka
}