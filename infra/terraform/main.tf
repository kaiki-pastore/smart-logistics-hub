terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  access_key                  = var.minio_user
  secret_key                  = var.minio_password
  region                      = "us-east-1"
  s3_use_path_style           = true
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    s3 = "http://localhost:9000"
  }
}

resource "aws_s3_bucket" "bronze" {
  bucket = "bronze"
}

resource "aws_s3_bucket" "silver" {
  bucket = "silver"
}

resource "aws_s3_bucket" "gold" {
  bucket = "gold"
}