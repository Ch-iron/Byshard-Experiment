variable "instance_type" {
  description = "The type of instance to use"
  type        = string
  default     = "t2.micro"
}

variable "node_count" {
  description = "The number of nodes to create in each region"
  type        = number
  default     = 1
}

variable "gateway_file" {
  type = set(string)
  default = [
    "../common/contracts",
    "../common/ca"
  ]
}