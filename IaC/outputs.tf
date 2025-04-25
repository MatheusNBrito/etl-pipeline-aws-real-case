output "ec2_public_dns" {
  value = aws_instance.etl_ec2.public_dns
}

output "s3_bucket_name" {
  value = aws_s3_bucket.etl_bucket.bucket
}

output "airflow_url" {
  value = "http://${aws_instance.etl_ec2.public_dns}:88"
}

output "ssh_command" {
  value = "ssh -i ~/.ssh/${var.key_name}.pem ubuntu@${aws_instance.etl_ec2.public_dns}"
}