"""Database 접속 Config 모듈"""

import os

# PostgreSQL
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Hadoop hdfs
HDFS_HOST = os.getenv("HDFS_HOST")
