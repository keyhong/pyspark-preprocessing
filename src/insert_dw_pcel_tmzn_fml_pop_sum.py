#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from datetime import timedelta
import logging
import time

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType

from soss.spark_session.db_connector import SparkClass
from soss.utils.config_parser import *

service_pcell_time_pop_schema = StructType([
    StructField("std_ymd", StringType(), True),
    StructField("hcode", StringType(), True),
    StructField("x_coord", StringType(), True),
    StructField("y_coord", StringType(), True),
    StructField("h_t_00", StringType(), True),
    StructField("h_t_01", StringType(), True),
    StructField("h_t_02", StringType(), True),
    StructField("h_t_03", StringType(), True),
    StructField("h_t_04", StringType(), True),
    StructField("h_t_05", StringType(), True),
    StructField("h_t_06", StringType(), True),
    StructField("h_t_07", StringType(), True),
    StructField("h_t_08", StringType(), True),
    StructField("h_t_09", StringType(), True),
    StructField("h_t_10", StringType(), True),
    StructField("h_t_11", StringType(), True),
    StructField("h_t_12", StringType(), True),
    StructField("h_t_13", StringType(), True),
    StructField("h_t_14", StringType(), True),
    StructField("h_t_15", StringType(), True),
    StructField("h_t_16", StringType(), True),
    StructField("h_t_17", StringType(), True),
    StructField("h_t_18", StringType(), True),
    StructField("h_t_19", StringType(), True),
    StructField("h_t_20", StringType(), True),
    StructField("h_t_21", StringType(), True),
    StructField("h_t_22", StringType(), True),
    StructField("h_t_23", StringType(), True),
    StructField("w_t_00", StringType(), True),
    StructField("w_t_01", StringType(), True),
    StructField("w_t_02", StringType(), True),
    StructField("w_t_03", StringType(), True),
    StructField("w_t_04", StringType(), True),
    StructField("w_t_05", StringType(), True),
    StructField("w_t_06", StringType(), True),
    StructField("w_t_07", StringType(), True),
    StructField("w_t_08", StringType(), True),
    StructField("w_t_09", StringType(), True),
    StructField("w_t_10", StringType(), True),
    StructField("w_t_11", StringType(), True),
    StructField("w_t_12", StringType(), True),
    StructField("w_t_13", StringType(), True),
    StructField("w_t_14", StringType(), True),
    StructField("w_t_15", StringType(), True),
    StructField("w_t_16", StringType(), True),
    StructField("w_t_17", StringType(), True),
    StructField("w_t_18", StringType(), True),
    StructField("w_t_19", StringType(), True),
    StructField("w_t_20", StringType(), True),
    StructField("w_t_21", StringType(), True),
    StructField("w_t_22", StringType(), True),
    StructField("w_t_23", StringType(), True),
    StructField("v_t_00", StringType(), True),
    StructField("v_t_01", StringType(), True),
    StructField("v_t_02", StringType(), True),
    StructField("v_t_03", StringType(), True),
    StructField("v_t_04", StringType(), True),
    StructField("v_t_05", StringType(), True),
    StructField("v_t_06", StringType(), True),
    StructField("v_t_07", StringType(), True),
    StructField("v_t_08", StringType(), True),
    StructField("v_t_09", StringType(), True),
    StructField("v_t_10", StringType(), True),
    StructField("v_t_11", StringType(), True),
    StructField("v_t_12", StringType(), True),
    StructField("v_t_13", StringType(), True),
    StructField("v_t_14", StringType(), True),
    StructField("v_t_15", StringType(), True),
    StructField("v_t_16", StringType(), True),
    StructField("v_t_17", StringType(), True),
    StructField("v_t_18", StringType(), True),
    StructField("v_t_19", StringType(), True),
    StructField("v_t_20", StringType(), True),
    StructField("v_t_21", StringType(), True),
    StructField("v_t_22", StringType(), True),
    StructField("v_t_23", StringType(), True),
    StructField("block_cd", StringType(), True)
])

service_pcell_sex_age_pop_schema = StructType([
    StructField("std_ymd", StringType(), True),
    StructField("hcode", StringType(), True),
    StructField("x_coord", StringType(), True),
    StructField("y_coord", StringType(), True),
    StructField("h_m_10", StringType(), True),
    StructField("h_m_20", StringType(), True),
    StructField("h_m_30", StringType(), True),
    StructField("h_m_40", StringType(), True),
    StructField("h_m_50", StringType(), True),
    StructField("h_m_60", StringType(), True),
    StructField("h_m_70", StringType(), True),
    StructField("h_w_10", StringType(), True),
    StructField("h_w_20", StringType(), True),
    StructField("h_w_30", StringType(), True),
    StructField("h_w_40", StringType(), True),
    StructField("h_w_50", StringType(), True),
    StructField("h_w_60", StringType(), True),
    StructField("h_w_70", StringType(), True),
    StructField("w_m_10", StringType(), True),
    StructField("w_m_20", StringType(), True),
    StructField("w_m_30", StringType(), True),
    StructField("w_m_40", StringType(), True),
    StructField("w_m_50", StringType(), True),
    StructField("w_m_60", StringType(), True),
    StructField("w_m_70", StringType(), True),
    StructField("w_w_10", StringType(), True),
    StructField("w_w_20", StringType(), True),
    StructField("w_w_30", StringType(), True),
    StructField("w_w_40", StringType(), True),
    StructField("w_w_50", StringType(), True),
    StructField("w_w_60", StringType(), True),
    StructField("w_w_70", StringType(), True),
    StructField("v_m_10", StringType(), True),
    StructField("v_m_20", StringType(), True),
    StructField("v_m_30", StringType(), True),
    StructField("v_m_40", StringType(), True),
    StructField("v_m_50", StringType(), True),
    StructField("v_m_60", StringType(), True),
    StructField("v_m_70", StringType(), True),
    StructField("v_w_10", StringType(), True),
    StructField("v_w_20", StringType(), True),
    StructField("v_w_30", StringType(), True),
    StructField("v_w_40", StringType(), True),
    StructField("v_w_50", StringType(), True),
    StructField("v_w_60", StringType(), True),
    StructField("v_w_70", StringType(), True),
    StructField("block_cd", StringType(), True)
])

if __name__ == "__main__":
        
     # line break
    line_break = "\n"
    
    # logging setting
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")
    logging.info("Main-Process : Start")
    start = time.time()
    
    # 인천 구 딕셔너리 (구 이름 : 구 코드)
    gu_dict = {
        "Bupyeong-gu": 28237,
        "Dong-gu": 28140,
        "Jung-gu": 28110,
        "Gyeyang-gu": 28245,
        "Namdong-gu": 28200,
        "Yeonsu-gu": 28185,
        "Michuhol-gu": 28177,
        "Seo-gu": 28260,
        "Ganghwa-gun": 28710,
        "Ongjin-gun": 28720,
    }

    for month in [ str(i).zfill(2) for i in range(7, 13) ]:

        logging.info(f"{month} Start --{line_break}")
    
        for gu_nm, gu_cd in gu_dict.items():
    
            logging.info(f"{gu_nm} Start --")
    
            # HDFS에서 인천 공간격자 유동인구 테이블 로딩
            incheon_service_pcell_time_pop = SparkClass.spark.read.csv(
                f"hdfs://{hdfs_host}:{hdfs_port}/shared/upload/FloatingPopulation/dsTmznPrnm/incheon_service_pcell_time_pop_2022{month}.csv",
                sep="|",
                schema=service_pcell_time_pop_schema,
                header=True
            )
    
            # 불필요 컬럼 드랍
            incheon_service_pcell_time_pop = incheon_service_pcell_time_pop.drop("block_cd")

            # 구별 필터링
            incheon_service_pcell_time_pop = incheon_service_pcell_time_pop.filter(F.col("hcode").like(f"{gu_cd}%"))
    
            # 인천 공간격자 정보 테이블 로딩
            sql = f"""
            SELECT /* 인천 공간격자 정보 */
                   cnt_x_crd AS x_coord
                 , cnt_y_crd AS y_coord
                 , grid_id
              FROM SOSS.IC_PCEL_STDR_INFO
             WHERE admd_cd LIKE "{gu_cd}%"
            """
            ic_pcell_df = SparkClass.jdbc_reader.option("query", sql).load()
            
            incheon_service_pcell_time_pop = incheon_service_pcell_time_pop.join(ic_pcell_df, how="left", on=["x_coord", "y_coord"])

            # Disk I/O를 줄이기 재사용 중간 변수 caching
            incheon_service_pcell_time_pop = incheon_service_pcell_time_pop.filter(F.col("grid_id").isNotNull()).cache()
    
            # 주거인구 컬럼 unpivot
            incheon_service_pcell_time_pop1 = incheon_service_pcell_time_pop.selectExpr(
                "std_ymd", "hcode", "x_coord", "y_coord", "grid_id",
                """stack(24, "00", h_t_00, "01", h_t_01, "02", h_t_02, "03", h_t_03, "04", h_t_04, "05", h_t_05,
                "06", h_t_06, "07", h_t_07, "08", h_t_08, "09", h_t_09, "10", h_t_10, "11", h_t_11,
                "12", h_t_12, "13", h_t_13, "14", h_t_14, "15", h_t_15, "16", h_t_16, "17", h_t_17,
                "18", h_t_18, "19", h_t_19, "20", h_t_20, "21", h_t_21, "22", h_t_22, "23", h_t_23) AS (tm, h_pop)"""
            )
            
            # 직장인구 컬럼 unpivot
            incheon_service_pcell_time_pop2 = incheon_service_pcell_time_pop.selectExpr(
                "std_ymd", "hcode", "x_coord", "y_coord",
                """stack(24, "00", w_t_00, "01", w_t_01, "02", w_t_02, "03", w_t_03, "04", w_t_04, "05", w_t_05,
                "06", w_t_06, "07", w_t_07, "08", w_t_08, "09", w_t_09, "10", w_t_10, "11", w_t_11,
                "12", w_t_12, "13", w_t_13, "14", w_t_14, "15", w_t_15, "16", w_t_16, "17", w_t_17,
                "18", w_t_18, "19", w_t_19, "20", w_t_20, "21", w_t_21, "22", w_t_22, "23", w_t_23) AS (tm, w_pop)"""
            )
    
            # 방문인구 컬럼 unpivot
            incheon_service_pcell_time_pop3 = incheon_service_pcell_time_pop.selectExpr(
                "std_ymd", "hcode", "x_coord", "y_coord",
                """stack(24, "00", v_t_00, "01", v_t_01, "02", v_t_02, "03", v_t_03, "04", v_t_04, "05", v_t_05,
                "06", v_t_06, "07", v_t_07, "08", v_t_08, "09", v_t_09, "10", v_t_10, "11", v_t_11,
                "12", v_t_12, "13", v_t_13, "14", v_t_14, "15", v_t_15, "16", v_t_16, "17", v_t_17,
                "18", v_t_18, "19", v_t_19, "20", v_t_20, "21", v_t_21, "22", v_t_22, "23", v_t_23) AS (tm, v_pop)"""
            )
    
    
            # 주거인구, 직장인구, 방문인구 컬럼 결합
            incheon_service_pcell_time_pop4 = incheon_service_pcell_time_pop1.join(incheon_service_pcell_time_pop2, how="left", on=["std_ymd", "tm", "hcode", "x_coord", "y_coord"])
            incheon_service_pcell_time_pop5 = incheon_service_pcell_time_pop4.join(incheon_service_pcell_time_pop3, how="left", on=["std_ymd", "tm", "hcode", "x_coord", "y_coord"])
            incheon_service_pcell_time_pop6 = incheon_service_pcell_time_pop5.filter("h_pop != 0 or w_pop != 0 or v_pop != 0")
    
            # 성별 연령별 유동인구 테이블 로딩
            incheon_service_pcell_sex_age_pop = SparkClass.spark.read.csv(
                f"hdfs://{hdfs_host}:{hdfs_port}/shared/upload/FloatingPopulation/dsAgrdePrnm/incheon_service_pcell_sex_age_pop_2022{month}.csv",
                sep="|",
                schema=service_pcell_sex_age_pop_schema,
                header=True
            )

            # 불필요 컬럼 드랍
            incheon_service_pcell_sex_age_pop = incheon_service_pcell_sex_age_pop.drop("block_cd")
            
            # 구별 필터링
            incheon_service_pcell_sex_age_pop = incheon_service_pcell_sex_age_pop.filter(F.col("hcode").like(f"{gu_cd}%"))
            
    
            for i in ("h", "w", "v"):
                # 주거인구, 직장인구, 방문인구 합계 산출
                incheon_service_pcell_sex_age_pop = incheon_service_pcell_sex_age_pop.withColumn(
                    colName=f"{i}_total",
                    col=F.col(f"{i}_m_10") + F.col(f"{i}_m_20") + F.col(f"{i}_m_30") + F.col(f"{i}_m_40") + F.col(f"{i}_m_50") + F.col(f"{i}_m_60") + F.col(f"{i}_m_70") \
                    + F.col(f"{i}_w_10") + F.col(f"{i}_w_20") + F.col(f"{i}_w_30") + F.col(f"{i}_w_40") + F.col(f"{i}_w_50") + F.col(f"{i}_w_60") + F.col(f"{i}_w_70")
                )
                
                # 주거인구, 직장인구, 방문인구 합계 산출
                drop_cols = [f"{i}_m_10", f"{i}_m_20", f"{i}_m_30", f"{i}_m_40", f"{i}_m_50", f"{i}_m_60", f"{i}_m_70"]
    
                # 남성 인구 컬럼 제거
                incheon_service_pcell_sex_age_pop = incheon_service_pcell_sex_age_pop.drop(*drop_cols)
    
                for j in ("10", "20", "30", "40", "50", "60", "70"):
                    # 연령별 유동인구 비율 산출
                    incheon_service_pcell_sex_age_pop = incheon_service_pcell_sex_age_pop.withColumn(f"{i}_w_{j}_r", F.when(F.col(f"{i}_total") == 0, 0).otherwise(F.col(f"{i}_w_{j}") / F.col(f"{i}_total")))
                    incheon_service_pcell_sex_age_pop = incheon_service_pcell_sex_age_pop.drop(f"{i}_w_{j}")
                
                # 주거인구, 직장인구, 방문인구 합계 컬럼 제거
                incheon_service_pcell_sex_age_pop = incheon_service_pcell_sex_age_pop.drop(f"{i}_total")
    
            # 와 성별연령별여성인구합계
            merge_pop1 = incheon_service_pcell_time_pop6.join(incheon_service_pcell_sex_age_pop, how="left", on=["std_ymd", "hcode",  "x_coord", "y_coord"])
            merge_pop2 = merge_pop1.drop("x_coord", "y_coord")
    
            for i in (("h", "rsd"), ("w", "wrc"), ("v", "vst")):
                for j in ("10", "20", "30", "40", "50", "60", "70"):
                    merge_pop2 = merge_pop2.withColumn(
                        colName=f"{i[1]}_ppl_fml_{j}_co",
                        col=F.col(f"{i[0]}_pop") * F.col(f"{i[0]}_w_{j}_r")
                    )
    
                    merge_pop2 = merge_pop2.drop(f"{i[0]}_w_{j}_r")
    
                merge_pop2 = merge_pop2.drop(f"{i[0]}_pop")
    
            merge_pop3 = merge_pop2.filter("h_w_10_r is not NULL")
    
            merge_pop4 = merge_pop3.withColumn(
                colName="pcel_tmzn_fml_co",
                col=F.col("rsd_ppl_fml_10_co") + F.col("rsd_ppl_fml_20_co") + F.col("rsd_ppl_fml_30_co") + F.col("rsd_ppl_fml_40_co") + F.col("rsd_ppl_fml_50_co") + F.col("rsd_ppl_fml_60_co") + F.col("rsd_ppl_fml_70_co") \
                + F.col("wrc_ppl_fml_10_co") + F.col("wrc_ppl_fml_20_co") + F.col("wrc_ppl_fml_30_co") + F.col("wrc_ppl_fml_40_co") + F.col("wrc_ppl_fml_50_co") + F.col("wrc_ppl_fml_60_co") + F.col("wrc_ppl_fml_70_co") \
                + F.col("vst_ppl_fml_10_co") + F.col("vst_ppl_fml_20_co") + F.col("vst_ppl_fml_30_co") + F.col("vst_ppl_fml_40_co") + F.col("vst_ppl_fml_50_co") + F.col("vst_ppl_fml_60_co") + F.col("vst_ppl_fml_70_co")
            )
    
            merge_pop5 = merge_pop4.drop(
                "rsd_ppl_fml_10_co", "rsd_ppl_fml_20_co", "rsd_ppl_fml_30_co", "rsd_ppl_fml_40_co", "rsd_ppl_fml_50_co", "rsd_ppl_fml_60_co", "rsd_ppl_fml_70_co",
                "wrc_ppl_fml_10_co", "wrc_ppl_fml_20_co", "wrc_ppl_fml_30_co", "wrc_ppl_fml_40_co", "wrc_ppl_fml_50_co", "wrc_ppl_fml_60_co", "wrc_ppl_fml_70_co",
                "vst_ppl_fml_10_co", "vst_ppl_fml_20_co", "vst_ppl_fml_30_co", "vst_ppl_fml_40_co", "vst_ppl_fml_50_co", "vst_ppl_fml_60_co", "vst_ppl_fml_70_co",
            )
    
            merge_pop6 = merge_pop5.filter("pcel_tmzn_fml_co != 0")
            
            merge_pop7 = merge_pop6.withColumn("gu_cd", F.substring("hcode", 1, 5))
            merge_pop8 = merge_pop7.drop("hcode")
                
            merge_pop9 = merge_pop8.withColumnRenamed("std_ymd", "stdr_de").withColumnRenamed("tm", "stdr_tm")
    
            merge_pop9.write.format("jdbc") \
                .option("url", f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_database}") \
                .option("driver", "org.postgresql.Driver") \
                .option("user", postgres_host) \
                .option("password", postgres_port) \
                .option("dbtable", "SOSS.DW_PCEL_TMZN_FML_POP_SUM") \
                .save(mode="append")
    
            logging.info(f"{gu_nm} End --{line_break}")
    
        logging.info(f"{month} End --{line_break}")
    
    end = time.time()
    elapsed_time = str(timedelta(seconds=end-start)).split(".")[0]
    logging.info(f"프로그램 소요시간 : {elapsed_time} --")
    logging.info("Main-Process : End")