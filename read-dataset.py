from pyspark.sql import SparkSession, Row
from pyspark.sql.types import IntegerType, StructType, StructField, StringType, FloatType, TimestampType
import time as timer
import pandas as pd
import numpy as np
import argparse
import datetime
import json
import os


#########################################################################
###########################  Utils  #####################################
#########################################################################


# Python 데이터 타입을 Spark SQL 데이터 타입으로 매핑
def get_spark_data_type(python_type):
    if python_type == int:
        return IntegerType()
    elif python_type == float:
        return FloatType()
    elif python_type == str:
        return StringType()
    elif python_type == datetime.datetime:
        return TimestampType()
    else:
        return StringType()  # 기본적으로 StringType을 사용

# 시작 시간을 저장하기 위한 전역 변수
start_time = None

def start_timer(description):
    global start_time
    start_time = timer.time()
    print(f"                     {description}")
    

def end_timer():
    if start_time is None:
        return "타이머가 시작되지 않았습니다."
    
    # 경과 시간 측정 및 반올림
    elapsed_time = timer.time() - start_time
    print(f"{elapsed_time:.6f}초")
    print("="*100)
    
class RequiredForClusterAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if getattr(namespace, 'mode') == 'cluster':
            setattr(namespace, self.dest, values)
        elif not getattr(namespace, self.dest):
            parser.error(f"{self.dest} is required when mode is 'cluster'")
            

            
#########################################################################
###########################  Utils  #####################################
#########################################################################

# argparse를 사용하여 명령줄 인자 처리 설정
parser = argparse.ArgumentParser(description='Process input arguments.')
parser.add_argument('--config', type=str, required=True, help='Path to the config.json file')
parser.add_argument('--mode', type=str, choices=['local', 'cluster'], required=True, help='Execution mode: local or cluster')
parser.add_argument('--image', type=str, action=RequiredForClusterAction, help='Full image path, required when mode is cluster')
args = parser.parse_args()

# --mode가 'cluster'이고 --image가 제공되지 않은 경우 에러 처리
if args.mode == 'cluster' and not args.image:
    parser.print_usage()  # 먼저 사용법 출력
    parser.error("--image is required when --mode is 'cluster'")  # 그 다음 커스텀 에러 메시지 출력

# config.json 파일 읽기
with open(args.config, 'r') as f:
    config = json.load(f)
    
# MongoDB URL 선택
if args.mode == 'local':
    mongo_url = config['EXTERNAL_MONGODB_URL']
else:
    mongo_url = config['K8S_INTERNAL_MONGODB_URL']

print(f"Running in {'local' if args.mode == 'local' else 'cluster'} mode")


# SparkSession 생성
spark = SparkSession.builder \
        .appName(config['SPARK_JOB_NAME']) \
        .getOrCreate()

# --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.2
# 를 사용했지만, 이 옵션은 위의 라이브러리 + 종속 라이브러리들을 드라이버 노드에만, 자동으로 다운로드함.
#
# 하지만, 워커노드에는 위 라이브러리들이 할당되지 않아, 코드가 cluster모드로 k8s에 제출되어도 작동하지않는 문제점이 있었음
# --jars <URL1>,<URL2>,<URL3>,... 를 이용하여 메이블 레포 링크를 걸어 마스터노드+워커노드 전부 jar 라이브러리를 할당할 수 있다.

spark.sparkContext.setLogLevel('WARN')

print("="*100)
print("FILES IN THIS DIRECTORY")
print(os.listdir(os.getcwd()))
print("="*100)


#########################################################################
###########################  Kafka  #####################################
#########################################################################

# # Kafka Bootstrap servers URL 선택
# if args.mode == 'local':
#     kafka_bootstrap_servers = ",".join(config['EXTERNAL_KAFKA_BOOTSTRAP_URLS'])
# else:
#     kafka_bootstrap_servers = ",".join(config['K8S_INTERNAL_KAFKA_BOOTSTRAP_URLS'])

# # Kafka 토픽 구독
# df = spark.readStream \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
#         .option("subscribe", "your-topic") \
#         .load()
        
# # 메시지 처리 로직 (여기에 실제 작업을 구현)
# def process_message(message):
#     # 메시지를 기반으로 하는 작업
#     pass

# # 메시지 처리
# query = df.writeStream.foreach(process_message).start()
# query.awaitTermination()


#########################################################################
###########################  Kafka  #####################################
#########################################################################


# # 원본 데이터 URL에서 보스턴 주택 가격 데이터 세트를 로드
# print("3. 원본 데이터 URL에서 보스턴 주택 가격 데이터 세트를 로드")
# data_url = "http://lib.stat.cmu.edu/datasets/boston"
# raw_df = pd.read_csv(data_url, sep="\s+", skiprows=22, header=None)
# data = np.hstack([raw_df.values[::2, :], raw_df.values[1::2, :2]])
# target = raw_df.values[1::2, 2]

# # 보스턴 주택 가격 데이터 세트의 컬럼 이름 정의
# print("4. 보스턴 주택 가격 데이터 세트의 컬럼 이름 정의")
# boston_columns = [
#     "CRIM", "ZN", "INDUS", "CHAS", "NOX", "RM", "AGE", "DIS", "RAD",
#     "TAX", "PTRATIO", "B", "LSTAT"
# ]

# # 데이터와 타겟을 결합하여 pandas DataFrame 생성
# boston_pdf = pd.DataFrame(data, columns=boston_columns)
# boston_pdf['PRICE'] = target

# # pandas DataFrame을 Spark DataFrame으로 변환
# print("5. pandas DataFrame을 Spark DataFrame으로 변환")
# boston_sdf = spark.createDataFrame(boston_pdf)
# print("="*100)
# print(boston_sdf.show(10))
# print("="*100)

# import matplotlib.pyplot as plt
# import seaborn as sns
# from datetime import datetime

# # 저장할 디렉토리 경로 설정
# directory = './plots/'

# # 디렉토리가 존재하지 않으면 생성
# if not os.path.exists(directory):
#     os.makedirs(directory)

# print("6. 시본의 regplot을 이용해 산점도와 선형 회귀 직선을 함께 표현")
# print("="*100)
# # 2개의 행과 4개의 열을 가진 subplots를 이용. axs는 4x2개의 ax를 가짐.
# fig, axs = plt.subplots(figsize=(16,8) , ncols=4 , nrows=2)
# lm_features = ['RM','ZN','INDUS','NOX','AGE','PTRATIO','LSTAT','RAD']
# colors = ['g', 'r', 'b', 'c', 'm', 'y', 'orange', 'darkblue' ]
# for i , feature in enumerate(lm_features):
#     row = int(i/4)
#     col = i%4
#     # 시본의 regplot을 이용해 산점도와 선형 회귀 직선을 함께 표현
#     sns.regplot(x=feature , y='PRICE',data=boston_pdf , ax=axs[row][col], color=colors[i])
    
# # 지정된 파일 이름으로 현재 그림을 저장
# timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
# filename = f'PRICE 산점도 & 선형 회귀 직선-{timestamp}.png'
# plt.savefig(os.path.join(directory, filename))
# print("="*100)


####################################################################################################
##################################     MongoDB Example      ########################################
####################################################################################################
from pyspark.sql import Row

# 예제 데이터 생성
print("7. MONGODB TEST / 예제 데이터 생성")
print("="*100)
data = [Row(name="noyusu", age=25), Row(name="noFlowWater", age=30)]
df = spark.createDataFrame(data)
print(df.show(10))
print("="*100)

# MongoDB URL 선택
if args.mode == 'local':
    mongo_url = config['EXTERNAL_MONGODB_URL']
else:
    mongo_url = config['K8S_INTERNAL_MONGODB_URL']

# MongoDB에 데이터 쓰기
print("8. MongoDB에 데이터 쓰기")
print(mongo_url)
print(config["MONGODB_DATABASE_NAME"])
print(config["MONGODB_COLLECTION_NAME"])
df.write.format("mongodb") \
    .option("spark.mongodb.write.connection.uri", mongo_url) \
    .option("spark.mongodb.write.database", config["MONGODB_DATABASE_NAME"]) \
    .option("spark.mongodb.write.collection", config["MONGODB_COLLECTION_NAME"]) \
    .mode("append").save()

print("="*100)

# MongoDB에서 데이터 읽기
print("9. MongoDB에서 데이터 읽기")
df_loaded = spark.read.format("mongodb") \
    .option("spark.mongodb.read.connection.uri", mongo_url) \
    .option("spark.mongodb.read.database", config["MONGODB_DATABASE_NAME"]) \
    .option("spark.mongodb.read.collection", config["MONGODB_COLLECTION_NAME"]) \
    .load()
print("="*100)

# 읽어온 데이터 출력
print("10. MongoDB에서 데이터 읽기")
df_loaded.show()
print("="*100)

####################################################################################################
##################################     MongoDB Example      ########################################
####################################################################################################

####################################################################################################
##################################         MongoDB          ########################################
####################################################################################################


    
# # MongoDB에서 데이터 읽기
# print("9. MongoDB에서 데이터 읽기")

# final_mongo_url=mongo_url+config["MONGODB_DATABASE_NAME"]+"."+config["MONGODB_COLLECTION_NAME"]
# print(final_mongo_url)

# df_loaded = spark.read.format("mongodb") \
#     .option("spark.mongodb.read.connection.uri", final_mongo_url) \
#     .load()
# print("="*100)

# # 읽어온 데이터 출력
# print("10. MongoDB에서 데이터 읽기")
# df_loaded.show()
# print("="*100)



####################################################################################################
##################################         MongoDB          ########################################
####################################################################################################

####################################################################################################
##################################         InfluxDB         ########################################
####################################################################################################
# content = "Initial influxdb_client"
# start_timer(content)

# import influxdb_client, time
# from influxdb_client import InfluxDBClient, Point, WritePrecision
# from influxdb_client.client.write_api import SYNCHRONOUS

# token = config['INFLUXDB_TOKEN']
# org = "influxdata"
# if args.mode == 'local':
#     url = config['EXTERNAL_INFLUXDB_URL']
# else:
#     url = config['K8S_INTERNAL_INFLUXDB_URL']
    
# bucket="kafka_test_bucket"
# client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)

# end_timer()
# ####################################################################################################
# content = "Initial query_api"
# start_timer(content)

# query_api = client.query_api()

# end_timer()
# ####################################################################################################
# content = "Start query"
# start_timer(content)

# query = """
# from(bucket: "kafka_test_bucket")
#   |> range(start: -20h)
#   |> filter(fn: (r) => r._measurement == "test")
# """
# tables = query_api.query(query, org="influxdata")

# end_timer()
# ####################################################################################################
# content = "각 레코드에 대한 다양한 정보를 추출합니다."
# start_timer(content)

# results = []    
# # 각 레코드에 대한 다양한 정보를 추출합니다.
# for table in tables:
#     for record in table.records:
#         query_start_time = record.get_start()
#         query_stop_time = record.get_stop()
#         measurement = record.get_measurement()
#         field = record.get_field()
#         value = record.get_value()
#         time = record.get_time()
        
#         # 태그 정보는 record.values에서 키를 사용하여 접근할 수 있습니다.
#         # 예를 들어, 'location'이라는 태그가 있다면, 다음과 같이 사용할 수 있습니다: record.values.get('location')
#         # 여기서는 임의의 'tag_name'을 사용했으나, 실제 태그 이름으로 교체해야 합니다.
#         tag_value = record.values.get('id')  # 실제 태그 이름으로 교체 필요
        
#         results.append({
#             "Measurement": measurement,
#             "Field": field,
#             "Value": value,
#             "Time": time,
#             "Start Time": query_start_time,
#             "Stop Time": query_stop_time,
#             "Tag Value": tag_value  # 태그 값이 없을 경우 None이 될 수 있습니다.
#         })
        
# end_timer()
# ####################################################################################################
# content = "Python 데이터 타입을 Spark SQL 데이터 타입으로 매핑 & 스키마 추론"
# start_timer(content)

# if results:
#     first_result = results[0]
#     fields = []
#     for field_name, value in first_result.items():
#         spark_data_type = get_spark_data_type(type(value))
#         fields.append(StructField(field_name, spark_data_type, True))
#     schema = StructType(fields)
# else:
#     schema = StructType([])
    
# end_timer()
# ####################################################################################################
# content = "results를 Row 객체로 변환해 스키마에 맞는 Spark DataFrame을 생성"
# start_timer(content)

# rows = [Row(**result) for result in results]
# df = spark.createDataFrame(rows, schema)

# end_timer()
# ####################################################################################################
# content = "Spark DataFrame 출력"
# start_timer(content)

# print(df.show())

# end_timer()
####################################################################################################
##################################         InfluxDB         ########################################
####################################################################################################
