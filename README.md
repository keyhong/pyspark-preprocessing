# pyspark-preprocessing

> Spark Dataframe API를 사용하여 전처리한 프로그램들의 코드 예시 모음집

# Descroption

HDFS/Postgres에서 유동인구 데이터를 소싱하여 전처리 및 테이블 결합 후 싱크하는 프로그램

- 매일 새벽에 airflow 배치 스케줄링을 통해, 새롭게 들어온 유동인구 데이터에 대한 전처리

- 만약, 새롭게 들어온 데이터가 존재하지 않는다면, 신규 파티션을 점검하는 부분에서 파이썬 프로세스를 exit 수행

- 새로 들어온 데이터에 대해서는 공간격자를 추가하고, 새로 만든 공간격자 데이터를 기존 공간격자 데이터에 append 수행

- 유동인구 데이터의 사이즈를 고려하여, 여러 달의 데이터가 들어오더라도 프로세스 수행시 일단 한달 분만 처리하여 적재 수행

# Caution

sys.path 또는 findspark를 사용하지 않는다면, OS 환경변수에 $SPARK_HOME이 있어야 하거나 파이썬 패키지를 빌드하여야 한다.

- 스파크 프로그램 설치시 default 로 설정하는 부분을 고려하여 OS.ENVIRON['SPARK_HOME'] 으로 spark 경로를 가져옴

- 만약 환경변수에 SPARK_HOME 이 없다면, find spark 또는 sys.path.insert(0, 스파크 경로) 를 프로세스 시작부분(global 정의 부분)에 코드 수정 필요
