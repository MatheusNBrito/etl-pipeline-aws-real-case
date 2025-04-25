provider "aws" {
  region = var.region
}

# Bucket S3 com boas práticas
resource "aws_s3_bucket" "etl_bucket" {
  bucket = "${var.project}-${var.environment}-bucket"

  tags = merge(var.tags, {
    Name = "${var.project}-s3-${var.environment}"
  })
}

# Configurações adicionais do S3
resource "aws_s3_bucket_versioning" "etl_bucket_versioning" {
  bucket = aws_s3_bucket.etl_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "etl_bucket_encryption" {
  bucket = aws_s3_bucket.etl_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Pastas no S3
resource "aws_s3_object" "folders" {
  for_each = toset(["raw/", "processed/", "gold/"])
  
  bucket = aws_s3_bucket.etl_bucket.bucket
  key    = each.value
  acl    = "private"
}

# EC2 melhorado
resource "aws_instance" "etl_ec2" {
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = "t2.micro"
  key_name                    = var.key_name
  associate_public_ip_address = true
  vpc_security_group_ids      = [aws_security_group.etl_sg.id]
  iam_instance_profile        = aws_iam_instance_profile.etl_profile.name
  user_data                   = templatefile("user_data.sh", {
    s3_bucket = aws_s3_bucket.etl_bucket.bucket
    airflow_port = 88 # Mantendo sua porta personalizada
  })
  subnet_id                   = aws_subnet.public.id

  root_block_device {
    encrypted   = true
    volume_size = 20
  }

  tags = merge(var.tags, {
    Name = "${var.project}-ec2-${var.environment}"
  })

  lifecycle {
    ignore_changes = [ami]
  }
}

# Security Group melhorado
resource "aws_security_group" "etl_sg" {
  name        = "${var.project}-sg-${var.environment}"
  description = "Security group controlado para ${var.project}"
  vpc_id      = aws_vpc.main.id

  # SSH restrito
  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.my_ip]
  }

  # Airflow Web (mantendo sua porta 88)
  ingress {
    description = "Airflow Webserver"
    from_port   = 88
    to_port     = 88
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Porta 81 mantida para compatibilidade
  ingress {
    description = "HTTP"
    from_port   = 81
    to_port     = 81
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

# IAM com políticas mais seguras
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

resource "aws_iam_instance_profile" "etl_profile" {
  name = "${var.project}-instance-profile-${var.environment}"
  role = aws_iam_role.etl_role.name
}

resource "aws_iam_role_policy" "etl_policy" {
  name = "${var.project}-s3-access-${var.environment}"
  role = aws_iam_role.etl_role.id
  
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:DeleteObject"
        ],
        Resource = [
          aws_s3_bucket.etl_bucket.arn,
          "${aws_s3_bucket.etl_bucket.arn}/*"
        ]
      },
      {
        Effect = "Allow",
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        Resource = "*"
      }
    ]
  })
}

# VPC básica
resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = merge(var.tags, {
    Name = "${var.project}-vpc"
  })
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.1.0/24"
  map_public_ip_on_launch = true

  tags = merge(var.tags, {
    Name = "${var.project}-public-subnet"
  })
}

resource "aws_internet_gateway" "gw" {
  vpc_id = aws_vpc.main.id

  tags = merge(var.tags, {
    Name = "${var.project}-igw"
  })
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.gw.id
  }

  tags = merge(var.tags, {
    Name = "${var.project}-public-rt"
  })
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

# AMI mais recente do Ubuntu
data "aws_ami" "ubuntu" {
  most_recent = true

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  owners = ["099720109477"] # Canonical
}

# CloudWatch para monitoramento
resource "aws_cloudwatch_metric_alarm" "ec2_cpu" {
  alarm_name          = "${var.project}-high-cpu-${var.environment}"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "2"
  metric_name         = "CPUUtilization"
  namespace           = "AWS/EC2"
  period              = "120"
  statistic           = "Average"
  threshold           = "80"
  alarm_description   = "Monitora uso alto de CPU na instância ETL"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  dimensions = {
    InstanceId = aws_instance.etl_ec2.id
  }

  tags = var.tags
}

resource "aws_sns_topic" "alerts" {
  name = "${var.project}-alerts-${var.environment}"
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}