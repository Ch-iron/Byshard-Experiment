output "communicator_publicip" {
  description = "The public IP address of the communicator instance in the Tokyo region"
  value       = aws_instance.communicator_tokyo.public_ip
}

output "node_publicips" {
  description = "A map of public IP addresses for the consensus nodes in the Tokyo region"
  value = aws_instance.node_tokyo
}