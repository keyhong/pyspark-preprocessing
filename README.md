# pyspark-preprocessing

> Spark Dataframe API를 사용하여 전처리 코드를 작성한 예시 모음집

- 1. Data Anylsis 분석 코드 : da-analysis-jupyter
- 2. ML 독립변수용 통계 데이터 생성 : pyspark-preprocessing


## `da-analysis-jupyter` Description

> 데이터 애널리스트 분석 코드를 지속적으로 업로드 할 전처리 소스코드 폴더


- 데이터 분석론과 물리 ERD의 따라 `Dimension Table` - `Fact Table` 간의 결합을 통한 obt 테이블을 먼저 생성해야 할 수도 있음

- 단순히 SQL 또는 프로그래밍으로 전처리하는 것이 중요한 것이 아니라, 멱등성이 있으면서 대규모 분산 처리에 스큐가 발생하지 않도록 동등한 `map`, `reduce` 작업이 발생되어야 함

- 해당 코드에서는 `groupBy` 집계의 기준(`by`에 사용되는 `dimension`)이 되는 컬럼을 데이터 소싱부터 분산해서 읽게 하여, 주요 집계시 `shuffle`을 최소화할 수 있도록 코드를 작성하였음

- 향후에도 `backfill` 등의 작업에서도 모든 파티션이 똑같이 분산처리를 수행하면서, 멱등성 있는 전처리를 계속 추가할 필요가 있음 (예정 및 보완 사항) 


## `pyspark-preprocessing` Description

> HDFS/Postgres에서 유동인구 데이터를 Sourcing하여 전처리 및 테이블 조인 후 Sink하는 소스코드 폴더

- 매일 새벽에 airflow 배치 스케줄링을 통해, 새롭게 들어온 유동인구 데이터에 대한 전처리

- 만약, 새롭게 들어온 데이터가 존재하지 않는다면, 신규 파티션을 점검하는 부분에서 파이썬 프로세스를 `exit()` 수행

- 새로 들어온 데이터에 대해서는 공간격자를 추가하고, 새로 만든 공간격자 데이터를 기존 공간격자 데이터에 `append` 수행

- 유동인구 데이터의 사이즈를 고려하여, 여러 달의 데이터가 들어오더라도 프로세스 수행시 일단 한달 분만 처리하여 적재 수행

## Caution

`sys.path` 또는 `findspark`를 사용하지 않는다면, OS 환경변수에 `$SPARK_HOME`이 있어야 하거나 파이썬 패키지를 빌드하여야 한다.

- 스파크 프로그램 설치시 default 로 설정하는 부분을 고려하여 `OS.ENVIRON["SPARK_HOME"]` 으로 spark 경로를 가져옴

- 만약 환경변수에 `$SPARK_HOME` 이 없다면, `findspark` 또는 `sys.path.insert(0, 스파크 경로)` 를 프로세스 시작부분(global 정의 부분)에 코드 수정 필요
