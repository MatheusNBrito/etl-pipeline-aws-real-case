provider "aws" {
  region = var.region
}

# Bucket S3 para dados
resource "aws_s3_bucket" "etl_bucket" {
  bucket = "${var.project}-${var.environment}-bucket"

  tags = merge(var.tags, {
    Name = "${var.project}-s3-${var.environment}"
  })
}

# Criando as "pastas" raw, processed, gold no S3 com aws_s3_object
resource "aws_s3_object" "raw" {
  bucket = aws_s3_bucket.etl_bucket.bucket
  key    = "raw/"
  acl    = "private"
}

resource "aws_s3_object" "processed" {
  bucket = aws_s3_bucket.etl_bucket.bucket
  key    = "processed/"
  acl    = "private"
}

resource "aws_s3_object" "gold" {
  bucket = aws_s3_bucket.etl_bucket.bucket
  key    = "gold/"
  acl    = "private"
}

# EC2 para execução dos jobs Spark e Airflow
resource "aws_instance" "etl_ec2" {
  ami                         = var.ami_id
  instance_type               = "t2.micro"
  key_name                    = var.key_name
  associate_public_ip_address = true
  vpc_security_group_ids      = [aws_security_group.etl_sg.id]
  iam_instance_profile        = aws_iam_instance_profile.etl_profile.name
  user_data                   = file("user_data.sh")

  tags = merge(var.tags, {
    Name = "${var.project}-ec2-${var.environment}"
  })
}

# Security Group para EC2
resource "aws_security_group" "etl_sg" {
  name        = "${var.project}-sg-${var.environment}"
  description = "Permitir SSH e acesso HTTP para ${var.project}"

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "HTTP"
    from_port   = 81
    to_port     = 81
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "HTTP"
    from_port   = 88
    to_port     = 88
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.project}-sg-${var.environment}"
  })
}

# IAM Role para EC2 acessar S3
resource "aws_iam_role" "etl_role" {
  name = "${var.project}-ec2-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version   = "2012-10-17",
    Statement = [
      {
        Effect    = "Allow",
        Principal = { Service = "ec2.amazonaws.com" },
        Action    = "sts:AssumeRole"
      }
    ]
  })

  tags = var.tags
}

# IAM Instance Profile
resource "aws_iam_instance_profile" "etl_profile" {
  name = "${var.project}-instance-profile-${var.environment}"
  role = aws_iam_role.etl_role.name
}

# Política da IAM Role para acesso ao S3
resource "aws_iam_role_policy" "etl_policy" {
  name = "${var.project}-s3-access-${var.environment}"
  role = aws_iam_role.etl_role.id
  policy = jsonencode({
    Version   = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = ["s3:*"],
        Resource = [
          aws_s3_bucket.etl_bucket.arn,
          "${aws_s3_bucket.etl_bucket.arn}/*"
        ]
      }
    ]
  })
}
