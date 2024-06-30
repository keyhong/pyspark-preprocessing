"""pyspark 유동인구 데이터 전처리 모듈"""

# -*- coding: utf-8 -*-

from __future__ import annotations

import logging
import warnings
from typing import List

import numpy as np
import pandas as pd
import pyspark
import pyspark.sql.functions as F
from pyproj import Proj, transform
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.utils import ParseException

warnings.filterwarnings("ignore")
logging.basicConfig(
    format="%(asctime)s: %(message)s", level=logging.INFO, datefmt="%H:%M:%S"
)

# PCELL_X(2019년 PCELL_X 최솟값) = pcell_info.min_x_crd.min() = 1077058.991787
PCELL_X: float = 1077058.991787

# PCELL_Y(2019년 PCELL_Y 최대값) = pcell_info.max_y_crd.max() = 1735054.147390
PCELL_Y: float = 1780604.147390


def transform_coord_format(df: pd.DataFrame, case: int = 0) -> pd.Series:
    """변환된 좌표주소를 반환하는 함수"""

    if case == 0:
        in_proj = Proj(init="EPSG:5179")
        out_proj = Proj(init="EPSG:4326")
    else:
        in_proj = Proj(init="EPSG:4326")
        out_proj = Proj(init="EPSG:5179")

    return pd.Series(transform(in_proj, out_proj, df[df.index[0]], df[df.index[1]]))


class QueryMixin:
    """소스 DBMS에서 질의할 쿼리 모음 클래스"""

    def get_cnt_cctv_info(self) -> pyspark.sql.DataFrame:
        """관제CCTV정보를 질의하는 함수"""

        query = """
        SELECT /* 관제CCTV정보 */
               la
             , lo
          FROM SOSS.CNT_CCTV_INFO
         WHERE lo BETWEEN 128.351 AND 128.761
           AND la BETWEEN 35.608 AND 36.016
        """

        # CCTV 데이터 py불러오기
        try:
            cctv_info_df = self.spark.sql(query)
        except ParseException:
            logging.error("Unable to process your query dude!!")
        except MemoryError:
            logging.error("Memory is full")
        else:
            cctv_info_df = cctv_info_df.dropna().toPandas()

        # EPSG:5179 => EPSG:4326으로 위경도 변환
        cctv_info_df[["lo", "la"]] = cctv_info_df.apply(
            transform_coord_format, case=1, axis=1
        )
        cctv_info_df["mtr_no"] = (
            ((cctv_info_df["lo"] - PCELL_X) / 50).astype(int).astype(str)
            + ","
            + ((PCELL_Y - cctv_info_df["la"]) / 50).astype(int).astype(str)
        )

        cctv_info_df = (
            cctv_info_df.groupby("mtr_no", as_index=False)["lo"]
            .count()
            .rename(columns={"lo": "cctv_co"})
        )

        # 스파크 데이터프레임으로 재변환
        cctv_info_df = self.spark.createDataFrame(cctv_info_df)

        return cctv_info_df

    def find_lastest_year_month(self) -> List[int]:
        """Returns list of dates can be loaded from ADHC.SERVICE_PCELL_TIME_POP to SOSS.DW_PCELL_TMZN_FPOP."""

        # 1. 가장 마지막 년월을 찾는다
        query = "SHOW PARTITIONS SOSS.DW_PCELL_TMZN_FPOP"

        try:
            lastest_year_month = self.spark.sql(query)
        except ParseException:
            logging.error("Unable to process your query dude!!")
        except MemoryError:
            logging.error("Memory is full")
        except ValueError:
            logging.error("data type is not changed")

        lastest_year_month = lastest_year_month.toPandas()
        lastest_year_month = int(lastest_year_month.values[-1][0][-6:])

        # 2. 새로 넣을 수 있는 데이터가 있는 지 확인한다.
        query = "SHOW PARTITIONS ADHC.SERVICE_PCELL_TIME_POP"

        try:
            pcel_partitions = self.spark.sql(query)
        except ParseException:
            logging.error("Unable to process your query dude!!")
        except MemoryError:
            logging.error("Memory is full")

        pcel_partitions = pcel_partitions.toPandas()

        pcel_partitions["partition"] = (
            pcel_partitions["partition"].str[-6:].astype(int)
        )

        # 3. 새로 넣을 수 있는 년월 데이터만 모아 리스트를 생성한다
        year_month_lst = [
            year_month
            for year_month in pcel_partitions["partition"]
            if year_month > lastest_year_month
        ]
        logging.info("추가 할 데이터의 년월 : %s", year_month_lst)

        return year_month_lst

    def get_pcel_stdr_info(self) -> pyspark.sql.DataFrame:
        """PCELL기준정보를 질의하는 함수"""

        query = """
        SELECT /* PCELL기준정보 */
               grid_id
             , mtr_no
          FROM SOSS.PCEL_STDR_INFO
        """

        try:
            pcel_stdr_info = self.spark.sql(query)
        except ParseException:
            logging.error("Unable to process your query dude!!")
        except MemoryError:
            logging.error("Memory is full")

        return pcel_stdr_info

    def get_last_pcel_id(self) -> int:
        """PCELL기준정보의 마지막 grid_id를 반환하는 함수"""

        query = "SELECT MAX(grid_id) FROM SOSS.PCEL_STDR_INFO"

        try:
            last_pcel_id = self.spark.sql(query)
        except ParseException:
            logging.error("Unable to process your query dude!!")
        except MemoryError:
            logging.error("Memory is full")
        except ValueError:
            logging.error("data type is not changed")

        last_pcel_id = last_pcel_id.toPandas()
        last_pcel_id = int(last_pcel_id.values[0][0][-6:])

        return last_pcel_id

    def get_service_pcell_time_pop(self, year_month: str) -> "pyspark.sql.DataFrame":
        """TODO"""

        # 서비스PCELL시간인구(PCELL)
        query = f"""
        SELECT /* 서비스PCELL시간인구(PCELL) */ *
          FROM ADHC.SERVICE_PCELL_TIME_POP
         WHERE pt_stdr_ym={year_month}
         """

        try:
            pcel_df = self.spark_session.sql(query)
        except ParseException:
            logging.error("Unable to process your query dude!!")
        except MemoryError:
            logging.error("Memory is full")

        drop_cols = ["pt_stdr_ym", "block_cd"]
        pcel_df = pcel_df.drop(*drop_cols)

        pcel_df = (
            pcel_df.withColumnRenamed("std_ymd", "stdr_de")
            .withColumnRenamed("hcode", "admd_cd")
            .withColumnRenamed("x_coord", "cnt_x_crd")
            .withColumnRenamed("y_coord", "cnt_y_crd")
        )

        return pcel_df


class Spark(QueryMixin):
    """spark 이용하여 DB 연결하는 클래스"""

    spark = (
        SparkSession.builder.appName("spark-preprocessing")
        .config("spark.driver.memory", "3g")
        .config("spark.executor.memory", "5g")
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
        .config("spark.hadoop.hive.exec.dynamic.partition", "true")
        .config("spark.sql.shuffle.partitions", 300)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .enableHiveSupport()
    )

    spark.sparkContext.setLogLevel("WARN")

def add_exists_pop_sum(spark_dataframe: pyspark.sql.DataFrame, time: str) -> pyspark.sql.DataFrame:
    """add_exists_pop_sum"""

    # pcel_stdr_info(PCELL기준정보) 데이터 로딩
    pcel_stdr_info: pyspark.sql.DataFrame = Spark.spark.get_pcel_stdr_info()

    # 기준일자-행정동코드-중심X좌표-중심Y좌표를 기준으로 유동인구 합계 데이터 생성
    pop_sum = spark_dataframe.groupBy(["stdr_de", "admd_cd", "cnt_x_crd", "cnt_y_crd"]).agg(
        F.sum("sum_" + time).alias("fpop_co")
    )

    # 중심X좌표, 중심Y좌표 컬럼을 이용하여 mtr_no(행렬번호) 생성
    pop_sum = pop_sum.withColumn(
        colName="mtr_no",
        col=F.concat(
            ((pop_sum["cnt_x_crd"] - PCELL_X) / 50).cast(IntegerType()),
            F.lit(","),
            ((PCELL_Y - pop_sum["cnt_y_crd"]) / 50).cast(IntegerType()),
        ),
    )

    # 기존 범위를 넘어가는 행렬번호는 제거하기
    pop_sum = pop_sum.where(
        (F.split(str=F.col("mtr_no"), pattern=",")[0] >= 0) & (F.split(str=F.col("mtr_no"), pattern=",")[1] <= 910)
    )

    # pop_sum 에 pcel_stdr_info 데이터 결합하기
    pop_sum = pop_sum.join(pcel_stdr_info, how="left", on="mtr_no")

    # stdr_tm(기준시간), gu_cd(구코드), pt_stdr_ym(파티션기준년월) 컬럼 생성하기
    pop_sum = pop_sum.withColumn("stdr_tm", F.lit(time).cast(StringType()))
    pop_sum = pop_sum.withColumn("gu_cd", F.col("admd_cd").substr(0, 5).cast(StringType()))
    pop_sum = pop_sum.withColumn("pt_stdr_ym", F.col("stdr_de").substr(0, 6).cast(StringType()))

    return pop_sum


def check_new_data_exist(spark_dataframe: pyspark.sql.DataFrame) -> bool:
    """새로운 데이터 있는 지 점검하는 함수"""

    # 그리드id 결측행(기존 데이터에 추가할 행렬번호) 유무 확인
    mtr_null_df = spark_dataframe.where(F.col("grid_id").isNull()).select(
        "stdr_de", "stdr_tm", "mtr_no", "admd_cd", "cnt_x_crd", "cnt_y_crd", "gu_cd", "fpop_co", "pt_stdr_ym"
    )

    if mtr_null_df.limit(1).count() < 1:
        mtr_null_df.where(F.col("grid_id").isNotNull()).select(
            "stdr_de", "stdr_tm", "grid_id", "admd_cd", "gu_cd", "fpop_co", "pt_stdr_ym"
        ).write.format("hive").mode("append").partitionBy("pt_stdr_ym").saveAsTable("SOSS.DW_PCELL_TMZN_FPOP")

        logging.info("추가 할 데이터가 존재하지 않습니다.")
        return True

    return False


def main():
    """main function"""

    logging.info("Main-Process : start")

    # 추가할 데이터의 파티션 년월 리스트
    appendable_year_months = Spark.spark.find_lastest_year_month()

    if not appendable_year_months:
        logging.info("No appendable data. exit.")
        exit()

    # 그리드별 CCTV개수 정보 데이터
    cctv_cnt_df = Spark.spark.get_cctv_location()

    # 0 ~ 23 (시간) 리스트 생성
    times = [format(x, "02") for x in range(24)]

    for year_month in appendable_year_months:
        logging.info("%s 시작 --", year_month)

        # PCELL단위_시간대별_유동인구 데이터 로딩
        pcell_time_pop_df = Spark.spark.get_service_pcell_time_pop(year_month)

        for time in times:
            pcell_time_pop_df = pcell_time_pop_df.withColumn(
                f"sum_{time}", sum([F.col(col_name) for col_name in [f"H_T_{time}", f"W_T_{time}", f"V_T_{time}"]])
            )

            drop_cols = [f"H_T_{time}", f"W_T_{time}", f"V_T_{time}"]
            pcell_time_pop_df = pcell_time_pop_df.drop(*drop_cols)

        for time in times:
            # 기존 행렬번호가 있는 데이터를 대상으로 유동인구 데이터 추가하기 """
            mtr_df = add_exists_pop_sum(pcell_time_pop_df, time)

            # 행렬번호가 기존 pcel_stdr_info(PCELL기준정보)에 없는 데이터는 새로 grid_id(그리드ID@pk)를 새로 추가하기 """
            if check_new_data_exist(mtr_df):
                continue

            logging.info("%s월 데이터를 추가합니다.", year_month)
            mtr_null_df = mtr_df.where(F.col("grid_id").isNull()).select(
                "stdr_de", "stdr_tm", "mtr_no", "admd_cd", "cnt_x_crd", "cnt_y_crd", "gu_cd", "fpop_co", "pt_stdr_ym"
            )
            unique_add_grid = (
                mtr_null_df.dropDuplicates(subset=["mtr_no"])
                .alias("unique_add_grid")
                .select("mtr_no", "admd_cd", "cnt_x_crd", "cnt_y_crd")
            )

            # 추가할 그리드 데이터 생성
            # 1. row_no(행번호), clm_no(열번호) 컬럼 생성
            unique_add_grid = unique_add_grid.withColumn(
                "row_no", F.split(str=F.col("mtr_no"), pattern=",")[0].cast(StringType())
            )
            unique_add_grid = unique_add_grid.withColumn(
                "clm_no", F.split(str=F.col("mtr_no"), pattern=",")[1].cast(StringType())
            )

            # 2. min_x_crd(최소X좌표), max_x_crd(최대X좌표), min_y_crd(최소Y좌표), max_y_crd(최대Y좌표) 컬럼 생성
            unique_add_grid = unique_add_grid.withColumn(
                "min_x_crd", (mtr_df["cnt_x_crd"] - 25).cast("DECIMAL(18, 8)")
            )
            unique_add_grid = unique_add_grid.withColumn(
                "max_x_crd", (mtr_df["cnt_x_crd"] + 25).cast("DECIMAL(18, 8)")
            )

            unique_add_grid = unique_add_grid.withColumn(
                "min_y_crd", (mtr_df["cnt_y_crd"] - 25).cast("DECIMAL(18, 8)")
            )
            unique_add_grid = unique_add_grid.withColumn(
                "max_y_crd", (mtr_df["cnt_y_crd"] + 25).cast("DECIMAL(18, 8)")
            )

            # 3. grid_id(그리드ID@pk) 컬럼 생성
            unique_add_grid = unique_add_grid.toPandas()

            # row_no, clm_no로 정렬
            unique_add_grid[["row_no", "clm_no"]] = unique_add_grid[["row_no", "clm_no"]].astype(int)
            unique_add_grid.sort_values(by=["row_no", "clm_no"], inplace=True)

            # pcel_stdr_info의 마지막 그리드 id
            last_pcel_id = Spark.spark.get_last_pcel_id()

            # 추가할 그리드 인덱스의 개수
            new_index_num = len(unique_add_grid)

            # 순서대로 grid_id 부여하기
            ids = np.arange(start=last_pcel_id + 1, stop=last_pcel_id + 1 + new_index_num).astype(str)
            ids = np.array(["GRID" + id_num.zfill(6) for id_num in ids])
            unique_add_grid["grid_id"] = ids

            # 좌표변환을 통해 wgs84 형식 좌표 추가 생성
            unique_add_grid[["cnt_lo", "cnt_la"]] = unique_add_grid[["cnt_x_crd", "cnt_y_crd"]].apply(
                transform_coord_format, case=0, axis=1
            )
            unique_add_grid[["min_lo", "min_la"]] = unique_add_grid[["min_x_crd", "min_y_crd"]].apply(
                transform_coord_format, case=0, axis=1
            )
            unique_add_grid[["max_lo", "max_la"]] = unique_add_grid[["max_x_crd", "max_y_crd"]].apply(
                transform_coord_format, case=0, axis=1
            )

            # 다시 spark.sql.DataFrame 형식으로 변환
            unique_add_grid = Spark.spark.spark_session.createDataFrame(unique_add_grid)

            # 그리드에 cctv 데이터를 결합하고, null인 데이터는 0으로 채우기
            unique_add_grid = unique_add_grid.join(cctv_cnt_df, how="left", on="mtr_no")
            unique_add_grid["cctv_co"].fillna(0, inplace=True)

            # 컬럼 순서 정렬
            unique_add_grid = unique_add_grid.select(
                "grid_id",  # 그리드ID@pk
                "row_no",  # 행번호
                "clm_no",  # 열번호
                "mtr_no",  # 행렬번호
                "admd_cd",  # 행정동코드
                "cnt_x_crd",  # 중심X좌표
                "cnt_y_crd",  # 중심Y좌표
                "min_x_crd",  # 최소X좌표
                "min_y_crd",  # 최소Y좌표
                "max_x_crd",  # 최대X좌표
                "max_y_crd",  # 최대Y좌표
                "cnt_lo",  # 중심경도
                "cnt_la",  # 중심위도
                "min_lo",  # 최소경도
                "min_la",  # 최소위도
                "max_lo",  # 최대경도
                "max_la",  # 최대위도
                "cctv_co",  # CCTV수
            )

            # 저장하기
            unique_add_grid.write.format("parquet").mode("append").saveAsTable("SOSS.PCEL_STDR_INFO")

            # 새로 추가된 행렬번호 데이터를 조회하여, 유동인구 데이터 추가하기
            # 공간격자 인데스 다시 불러오기
            pcel_stdr_info = Spark.spark.get_pcel_stdr_info()

            # 변경된 PCELL기준정보 데이터에 mtr_df 데이터를 결합하여 PCELL단위시간대별유동인구 데이터 적재
            mtr_df.where(F.col("grid_id").isNotNull()).select(
                "stdr_de", "stdr_tm", "grid_id", "admd_cd", "gu_cd", "fpop_co", "pt_stdr_ym"
            ).write.format("hive").mode("append").partitionBy("pt_stdr_ym").saveAsTable("SOSS.DW_PCELL_TMZN_FPOP")

            del mtr_df, unique_add_grid, mtr_null_df, pcel_stdr_info

    logging.info("-------------------- 프로그램 종료 --")

if __name__ == "__main__":
    main()