"""pyspark 유동인구 데이터 전처리 모듈"""

# -*- coding: utf-8 -*-

from __future__ import annotations

import calendar
import logging
import warnings
from collections import namedtuple
from typing import Dict, Iterable, List

import pyspark
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from utils.config_parser import (
    POSTGRES_DATABASE,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_USER
)

warnings.filterwarnings("ignore")
logging.basicConfig(format="%(asctime)s: %(message)s", level=logging.INFO, datefmt="%H:%M:%S")

def get_year_month_day_lst(year: int = 2022) -> List[str]:
    """입력 파라미터 연도의 연월(YYYYMMD) 일자를 반환하는 함수"""
    
    if not isinstance(year, int):
        raise ValueError("year must be int")

    Date = namedtuple("yearMonthDay", ["first_date", "last_date"])

    date_lst: List[Date] = []

    for month in range(1, 13):
        last_day = calendar.monthrange(year, month)[1]

        first_date = "".join([str(year), str(month).zfill(2), "01"])
        last_date = "".join([str(year), str(month).zfill(2), str(last_day).zfill(2)])

        date_lst.append(Date(first_date, last_date))

    return date_lst


def melt(
    pyspark_data_frame: pyspark.sql.DataFrame,
    id_vars: Iterable[str],
    value_vars: Iterable[str],
    var_name: str = "variable",
    value_name: str = "value",
) -> pyspark.sql.DataFrame:
    """
    Convert :class:`DataFrame` from wide to long format.
    """

    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = F.array(*(F.struct(F.lit(c).alias(var_name), F.col(c).alias(value_name)) for c in value_vars))

    # Add to the DataFrame and explode
    _tmp = pyspark_data_frame.withColumn("_vars_and_vals", F.explode(_vars_and_vals))

    cols = id_vars + [F.col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]

    return _tmp.select(*cols)

def main():
    """main function"""

    logging.info("Main-Process : Start")

    gu_dict: Dict[str, int] = {
        "Bupyeong-gu": 28237,
        "Dong-gu": 28140,
        "Jung-gu": 28110,
        "Gyeyang-gu": 28245,
        "Namdong-gu": 28200,
        "Ongjin-gun": 28720,
        "Yeonsu-gu": 28185,
        "Michuhol-gu": 28177,
        "Seo-gu": 28260,
        "Ganghwa-gun": 28710,
    }

    # SparkSession build. (spark deploymode : local)
    spark = (
        SparkSession.builder.appName("soss2.0")
        .master("local[*]")
        .config("spark.jars", "/opt/spark/postgresql-42.5.4.jar")
        .config("spark.driver.memory", "24g")
        .config("spark.ui.port", "8080")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    for _, gu_cd in gu_dict.items():
        for first_date, last_date in get_year_month_day_lst():
            sql = f"""
            SELECT std_ymd, x_coord, y_coord, hcode
                 , h_t_00, h_t_01, h_t_02, h_t_03, h_t_04, h_t_05, h_t_06, h_t_07
                 , h_t_08, h_t_09, h_t_10, h_t_11, h_t_12, h_t_13, h_t_14, h_t_15
                 , h_t_16, h_t_17, h_t_18, h_t_19, h_t_20, h_t_21, h_t_22, h_t_23
                 , w_t_00, w_t_01, w_t_02, w_t_03, w_t_04, w_t_05, w_t_06, w_t_07
                 , w_t_08, w_t_09, w_t_10, w_t_11, w_t_12, w_t_13, w_t_14, w_t_15
                 , w_t_16, w_t_17, w_t_18, w_t_19, w_t_20, w_t_21, w_t_22, w_t_23
                 , v_t_00, v_t_01, v_t_02, v_t_03, v_t_04, v_t_05, v_t_06, v_t_07
                 , v_t_08, v_t_09, v_t_10, v_t_11, v_t_12, v_t_13, v_t_14, v_t_15
                 , v_t_16, v_t_17, v_t_18, v_t_19, v_t_20, v_t_21, v_t_22, v_t_23
              FROM ADHC.INCHEON_SERVICE_PCELL_TIME_POP
             WHERE std_ymd BETWEEN '{first_date}' AND '{last_date}'
               AND hcode LIKE '{gu_cd}%'
            """

            # get data from postgres db
            df = (
                spark.read.format("jdbc")
                .option("url", f"jdbc:postgresql://{POSTGRES_HOST}/{POSTGRES_DATABASE}")
                .option("user", POSTGRES_USER)
                .option("password", POSTGRES_PASSWORD)
                .option("query", sql)
                .load()
            )

            # make column "gu_cd"
            df = df.withColumn("gu_cd", F.col("hcode").substr(1, 5))

            # drop columns
            df = df.drop(*["hcode", "block_cd"])

            # rename columns
            df = (
                df.withColumnRenamed("std_ymd", "stdr_de")
                .withColumnRenamed("x_coord", "cnt_x_crd")
                .withColumnRenamed("y_coord", "cnt_y_crd")
            )

            # 시간별 주거 + 직장 + 방문 합계 산출

            # 00 ~ 23시간 리스트 산출
            tms: List[str] = [str(tm).zfill(2) for tm in range(24)]

            # make column "tm" (tatal pop by hour) & drop column "H_T_{tm}", "W_T_{tm}", "V_T_{tm}"
            for tm in tms:
                df = df.withColumn(
                    f"{tm}", sum([F.col(col_name) for col_name in [f"H_T_{tm}", f"W_T_{tm}", f"V_T_{tm}"]])
                )
                drop_cols = [f"H_T_{tm}", f"W_T_{tm}", f"V_T_{tm}"]
                df = df.drop(*drop_cols)

            min_x_crd = 746758.991784
            min_y_crd = 1883404.14739

            # make column "row_id"
            pcell_time_pop_matrix = df.withColumn(
                colName="row_id",
                col=(
                    (F.col("cnt_x_crd").cast("DECIMAL(38, 18)") - F.lit(min_x_crd).cast("DECIMAL(38, 18)"))
                    / F.lit(50).cast("DECIMAL(38, 18)")
                ).cast("Integer"),
            )

            # make column "col_id"
            pcell_time_pop_matrix = pcell_time_pop_matrix.withColumn(
                colName="col_id",
                col=(
                    (F.col("cnt_y_crd").cast("DECIMAL(38, 18)") - F.lit(min_y_crd).cast("DECIMAL(38, 18)"))
                    / F.lit(50).cast("DECIMAL(38, 18)")
                ).cast("Integer"),
            )

            # drop column "cnt_x_crd", "cnt_y_crd"
            pcell_time_pop_matrix = pcell_time_pop_matrix.drop(*["cnt_x_crd", "cnt_y_crd"])

            # make column "mtr_no"
            pcell_time_pop_matrix = pcell_time_pop_matrix.withColumn(
                "mtr_no", F.concat_ws(",", F.col("row_id"), F.col("col_id"))
            )

            # melt
            pcell_time_pop_melt = melt(
                pyspark_data_frame=pcell_time_pop_matrix,
                id_vars=["stdr_de", "mtr_no", "gu_cd"],
                var_name="stdr_tm",
                value_vars=[f"{i:02}" if i < 10 else str(i) for i in range(24)]
            )

            # make column "fpop_co" (groupby value)
            pcell_time_pop = pcell_time_pop_melt.groupBy(["stdr_de", "stdr_tm", "mtr_no", "gu_cd"]).agg(
                F.sum("value").alias("fpop_co")
            )

            sql = "SELECT grid_id, mtr_no FROM SOSS.IC_PCEL_STDR_INFO"

            pcell_stdr_info = (
                spark.read.format("jdbc")
                .option("url", f"jdbc:postgresql://{POSTGRES_HOST}/{POSTGRES_DATABASE}")
                .option("user", POSTGRES_USER)
                .option("password", POSTGRES_PASSWORD)
                .option("query", sql)
                .load()
            )

            df_join = pcell_time_pop.join(other=pcell_stdr_info, on="mtr_no", how="left")
            df_join = df_join.drop(*["mtr_no"])

            # load output data (spark action)
            df_join.write.jdbc(
                url=f"jdbc:postgresql://{POSTGRES_HOST}/{POSTGRES_DATABASE}",
                table="SOSS.DW_IC_PCELL_TMZN_FPOP",
                mode="append",
                properties={"user": POSTGRES_USER, "password": POSTGRES_PASSWORD},
            )

    logging.info("Main-Process : End")    


if __name__ == "__main__":
    main()
