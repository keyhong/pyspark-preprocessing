"""
spark 이용하여 db 연결하는 모듈
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from soss.utils.config_parser import (
    postgres_host,
    postgres_port,
    postgres_database,
    postgres_user,
    postgres_password
)

__all__ = ["SparkClass"]

class SparkClass:
    """
        pyspark local mode로 실행. cluster mode(YARN, Standalone, Mesos, Kubernetes) 사용시
        spark-env.sh & spark-defaults.conf 설정 및 애플리케이션 코드에서 master config 수정 필요
    """
    
    spark = SparkSession.builder \
        .appName("safe2.0") \
        .master("local[*]") \
        .config("spark.jars", os.path.join(os.path.dirname(os.path.abspath(__file__)), "postgresql-42.6.0.jar")) \
        .config("spark.driver.memory", "32g") \
        .getOrCreate()
    
    # pyspark log level setting
    spark.sparkContext.setLogLevel("ERROR")

    # Postgresql에 jdbc로 연결한 pyspark.sql.readwriter 객체를 생성
    jdbc_reader = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_database}") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", postgres_user) \
        .option("password", postgres_password)

    def __del__(cls):
        cls.spark.stop()