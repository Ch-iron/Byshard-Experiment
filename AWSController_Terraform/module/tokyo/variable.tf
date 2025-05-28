variable "instance_type" {
  description = "The type of instance to use"
  type        = string
}

variable "node_count" {
  description = "The number of nodes to create in each region"
  type        = number
}

variable "tls_private_key" {
  description = "The TLS private key for the CA"
  type        = string
}

variable "private_key" {
  description = "Path to the private key file"
  type        = string
}