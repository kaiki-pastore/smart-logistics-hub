variable "minio_user" {
  description = "Root user from MinIO"
  type        = string
}

variable "minio_password" {
  description = "Root password from MinIO"
  type        = string
  sensitive   = true
}