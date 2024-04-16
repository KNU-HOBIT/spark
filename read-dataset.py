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
    
print(f"Running in {'local' if args.mode == 'local' else 'cluster'} mode")
    
# MongoDB URL 선택
if args.mode == 'local':
    mongo_url = config['EXTERNAL_MONGODB_URL']
else:
    mongo_url = config['K8S_INTERNAL_MONGODB_URL']
    
# SparkSession 생성
spark = SparkSession.builder \
        .appName(config['SPARK_JOB_NAME']) \
        .config("spark.mongodb.input.partitioner", "com.mongodb.spark.sql.connector.read.partitioner.PaginateIntoPartitionsPartitioner") \
        .getOrCreate()
spark.sparkContext.setLogLevel('WARN')


# --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.2
# 를 사용했지만, 이 옵션은 위의 라이브러리 + 종속 라이브러리들을 드라이버 노드에만, 자동으로 다운로드함.
#
# 하지만, 워커노드에는 위 라이브러리들이 할당되지 않아, 코드가 cluster모드로 k8s에 제출되어도 작동하지않는 문제점이 있었음
# --jars <URL1>,<URL2>,<URL3>,... 를 이용하여 메이블 레포 링크를 걸어 마스터노드+워커노드 전부 jar 라이브러리를 할당할 수 있다.

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

#########################################################################
###########################  Spark Example  #############################
#########################################################################


# 원본 데이터 URL에서 보스턴 주택 가격 데이터 세트를 로드
# print("1. 원본 데이터 URL에서 보스턴 주택 가격 데이터 세트를 로드")
# data_url = "http://lib.stat.cmu.edu/datasets/boston"
# raw_df = pd.read_csv(data_url, sep="\s+", skiprows=22, header=None)
# data = np.hstack([raw_df.values[::2, :], raw_df.values[1::2, :2]])
# target = raw_df.values[1::2, 2]

# # 보스턴 주택 가격 데이터 세트의 컬럼 이름 정의
# print("2. 보스턴 주택 가격 데이터 세트의 컬럼 이름 정의")
# boston_columns = [
#     "CRIM", "ZN", "INDUS", "CHAS", "NOX", "RM", "AGE", "DIS", "RAD",
#     "TAX", "PTRATIO", "B", "LSTAT"
# ]

# # 데이터와 타겟을 결합하여 pandas DataFrame 생성
# boston_pdf = pd.DataFrame(data, columns=boston_columns)
# boston_pdf['price'] = target

# start_timer("START!")
# # pandas DataFrame을 Spark DataFrame으로 변환
# print("3. pandas DataFrame을 Spark DataFrame으로 변환")
# boston_sdf = spark.createDataFrame(boston_pdf)
# print("="*100)
# print(boston_sdf.show(10))
# print("="*100)

# ### Feature Vectorization 적용하고 학습과 테스트 데이터 세트로 분할
# print("4. Feature Vectorization 적용하고 학습과 테스트 데이터 세트로 분할")
# from pyspark.ml.feature import VectorAssembler

# vector_assembler = VectorAssembler(inputCols=boston_columns, outputCol='features')
# boston_sdf_vectorized = vector_assembler.transform(boston_sdf)
# train_sdf, test_sdf = boston_sdf_vectorized.randomSplit([0.7, 0.3], seed=2021)
# print("="*100)
# train_sdf.limit(10)
# print("="*100)
# ### LinearRegression 학습, 예측, 평가 수행. 
# print("5. LinearRegression 학습, 예측, 평가 수행.")
# from pyspark.ml.regression import LinearRegression

# lr = LinearRegression(featuresCol='features', labelCol='price', 
#                          maxIter=100, regParam=0)
# lr_model = lr.fit(train_sdf)
# lr_predictions = lr_model.transform(test_sdf)
# print("="*100)
# lr_predictions.limit(10)
# print("="*100)

# # Ridge, Lasso, ElasticNet 학습,예측 테스트를 위해서 함수 생성. 
# print("6. Ridge, Lasso, ElasticNet 학습,예측 테스트를 위해서 함수 생성. ")
# def do_train_predict(lr_estimator, train_sdf, test_sdf):
#     lr_model = lr_estimator.fit(train_sdf)
#     predictions = lr_model.transform(test_sdf)
#     return lr_model, predictions

# lr_model, lr_predictions = do_train_predict(lr, train_sdf, test_sdf)
# print("="*100)

# from pyspark.ml.evaluation import RegressionEvaluator

# def get_reg_eval(predictions):
#     mse_eval = RegressionEvaluator(labelCol='price', predictionCol='prediction', metricName='mse')
#     rmse_eval = RegressionEvaluator(labelCol='price', predictionCol='prediction', metricName='rmse')
#     r2_eval = RegressionEvaluator(labelCol='price', predictionCol='prediction', metricName='r2')

#     print('mse:', mse_eval.evaluate(predictions), 'rmse:', rmse_eval.evaluate(predictions), 'r2:', r2_eval.evaluate(predictions))
    
# get_reg_eval(lr_predictions)
# print("="*100)


# # LinearRegression은 학습 데이터를 기본으로 정규화 scaling변환. standardization=True가 기본값.
# print("7. LinearRegression은 학습 데이터를 기본으로 정규화 scaling변환. standardization=True가 기본값.") 
# lr_model.extractParamMap()


# # 학습 데이터를 학습전에 정규환 변환 적용하지 않음. 
# lr = LinearRegression(featuresCol='features', labelCol='price', 
#                          maxIter=100, regParam=0, standardization=False)
# lr_model, lr_predictions = do_train_predict(lr, train_sdf, test_sdf)
# get_reg_eval(lr_predictions)
# print("="*100)



# # linear regression의 회귀계수와 절편은 각각 EstimatorModel의 coefficients와 intercept에서 확인 
# print('8. linear regression의 회귀계수와 절편은 각각 EstimatorModel의 coefficients와 intercept에서 확인 ')
# print('회귀 계수:', lr_model.coefficients)
# print('회귀 절편:', lr_model.intercept)
# coeff = pd.Series(data=lr_model.coefficients, index=boston_columns)
# print(coeff.sort_values(ascending=False))

# import matplotlib.pyplot as plt
# import seaborn as sns

# def get_coefficient(coefficients, columns):
#     coeff = pd.Series(data=coefficients, index=columns).sort_values(ascending=False)
#     print(coeff)
#     # sns.barplot(x=coeff.values, y=coeff.index)
#     # plt.show()
# get_coefficient(lr_model.coefficients, boston_columns)
# print("="*100)


# print("####규제 선형회귀 적용")
# print("regParam = 0, elasticNetParam = 0 => 무규제 선형회귀")
# print("regParam > 0, elasticNetParam = 0 => Ridge(L2 규제)")
# print("regParam > 0, elasticNetParam = 1 => Lasso(L1 규제)")
# print("regParam > 0, elasticNetParam = (0 ~ 1) => ElasticNet")
# #### 규제 선형회귀 적용
# # regParam = 0, elasticNetParam = 0 => 무규제 선형회귀
# # regParam > 0, elasticNetParam = 0 => Ridge(L2 규제)
# # regParam > 0, elasticNetParam = 1 => Lasso(L1 규제)
# # regParam > 0, elasticNetParam = (0 ~ 1) => ElasticNet

# def do_train_predict(lr_estimator, train_sdf, test_sdf):
#     lr_model = lr_estimator.fit(train_sdf)
#     predictions = lr_model.transform(test_sdf)
#     return lr_model, predictions

# print("regParam=5, elasticNetParam=0으로 alpha값이 5인 Ridge Estimator 생성. ")
# from pyspark.ml.regression import LinearRegression

# # regParam=5, elasticNetParam=0으로 alpha값이 5인 Ridge Estimator 생성. 
# ridge = LinearRegression(featuresCol='features', labelCol='price', 
#                          maxIter=100, regParam=5, elasticNetParam=0)

# ridge_model, ridge_predictions = do_train_predict(ridge, train_sdf, test_sdf)
# get_reg_eval(ridge_predictions)
# get_coefficient(ridge_model.coefficients, boston_columns)
# # mse: 21.564913322698093 rmse: 4.643803755834014 r2: 0.7383398763030963
# # mse: 23.721435164891943 rmse: 4.8704656004217854 r2: 0.7121734937380622
# print("="*100)
# print("regParam=0.01, elasticNetParam=1으로 alpha값이 0.1인 Lasso Estimator 생성. ")
# from pyspark.ml.regression import LinearRegression

# # regParam=0.01, elasticNetParam=1으로 alpha값이 0.1인 Lasso Estimator 생성. 
# lasso = LinearRegression(featuresCol='features', labelCol='price', 
#                          maxIter=100, regParam=0.1, elasticNetParam=1)

# lasso_model, lasso_predictions = do_train_predict(lasso, train_sdf, test_sdf)
# get_reg_eval(lasso_predictions)
# get_coefficient(lasso_model.coefficients, boston_columns)
# print("="*100)

# print("regParam=20, elasticNetParam=0.1으로 a+b=20, L1 ratio=0.1임. a/(a+b) = 2/20, L1 alpha값이 2, L2 alpha값이 18인 ElasticNet Estimator 생성. ")
# from pyspark.ml.regression import LinearRegression

# # regParam=20, elasticNetParam=0.1으로 a+b=20, L1 ratio=0.1임. a/(a+b) = 2/20, L1 alpha값이 2, L2 alpha값이 18인 ElasticNet Estimator 생성. 
# elastic_net = LinearRegression(featuresCol='features', labelCol='price', 
#                          maxIter=100, regParam=20, elasticNetParam=0.1)

# elastic_net_model, elastic_net_predictions = do_train_predict(elastic_net, train_sdf, test_sdf)
# get_reg_eval(elastic_net_predictions)
# get_coefficient(elastic_net_model.coefficients, boston_columns)
# print("="*100)

# print("전체 컬럼에 Standard Scaler 적용. scaling은 vectorized된 feature에만 가능. feature vectorization 적용후 standard scaling 적용. ")
# from pyspark.ml.feature import StandardScaler
# from pyspark.ml.feature import VectorAssembler

# # 전체 컬럼에 Standard Scaler 적용. scaling은 vectorized된 feature에만 가능. feature vectorization 적용후 standard scaling 적용. 
# vec_assembler = VectorAssembler(inputCols=boston_columns, outputCol='features')
# standard_scaler = StandardScaler(inputCol='features', outputCol='scaled_features')

# boston_sdf_vectorized = vec_assembler.transform(boston_sdf)
# boston_sdf_vect_scaled = standard_scaler.fit(boston_sdf_vectorized).transform(boston_sdf_vectorized)

# boston_sdf_vect_scaled.limit(10)
# print("="*100)

# print("feature vectorization->scaling이 적용된 데이터 세트를 학습과 테스트로 분리 ")
# train_sdf_scaled, test_sdf_scaled = boston_sdf_vect_scaled.randomSplit([0.7, 0.3], seed=2021)

# print("featuresCol이 features가 아닌 scaled_features가 되어야함. ")
# ridge_scale = LinearRegression(featuresCol='scaled_features', labelCol='price', 
#                          maxIter=100, regParam=5, elasticNetParam=0, standardization=False)

# # scaled된 학습과 테스트 데이터 세트 입력하여 lasso 학습/예측/평가
# print("scaled된 학습과 테스트 데이터 세트 입력하여 lasso 학습/예측/평가")
# ridge_model, ridge_predictions = do_train_predict(ridge_scale, train_sdf_scaled, test_sdf_scaled)
# get_reg_eval(ridge_predictions)
# get_coefficient(ridge_model.coefficients, boston_columns)
# print("="*100)
# #mse: 21.564913322698093 rmse: 4.643803755834014 r2: 0.7383398763030963


# ### 회귀 트리 적용
# # * DecisionTreeRegressor, RandomForestRegressor, GBTRegressor 에서 RandomForestRegressor만 적용해봄.
# print("### 회귀 트리 적용")
# print("* DecisionTreeRegressor, RandomForestRegressor, GBTRegressor 에서 RandomForestRegressor만 적용해봄.")


# from pyspark.ml.regression import RandomForestRegressor

# rf = RandomForestRegressor(featuresCol='features', labelCol='price', 
#                                maxDepth=5, numTrees=10)
# rf_model, rf_predictions = do_train_predict(rf, train_sdf, test_sdf)
# get_reg_eval(rf_predictions)
# print("="*100)


# from pyspark.ml.linalg import DenseVector
# import matplotlib.pyplot as plt
# import seaborn as sns


# print("feature importance가 Sparse Vector이므로 Dense Vector로 변환.")
# rf_ftr_importances_list = list(DenseVector(rf_model.featureImportances))

# ftr_importances = pd.Series(data=rf_ftr_importances_list, index=boston_columns).sort_values(ascending=False)
# print(ftr_importances)
# # sns.barplot(x=ftr_importances.values, y=ftr_importances.index)
# # plt.show()
# print("="*100)


# end_timer()


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

#########################################################################
###########################  Spark Example  #############################
#########################################################################

####################################################################################################
##################################     MongoDB Example      ########################################
####################################################################################################
# from pyspark.sql import Row

# # 예제 데이터 생성
# print("7. MONGODB TEST / 예제 데이터 생성")
# print("="*100)
# data = [Row(name="noyusu", age=25), Row(name="noFlowWater", age=30)]
# df = spark.createDataFrame(data)
# print(df.show(10))
# print("="*100)


# # MongoDB에 데이터 쓰기
# print("8. MongoDB에 데이터 쓰기")
# df.write.format("mongodb") \
#     .option("spark.mongodb.write.connection.uri", mongo_url) \
#     .option("spark.mongodb.write.database", config["MONGODB_DATABASE_NAME"]) \
#     .option("spark.mongodb.write.collection", config["MONGODB_COLLECTION_NAME"]) \
#     .mode("append").save()

# print("="*100)

# # MongoDB에서 데이터 읽기
# print("9. MongoDB에서 데이터 읽기")
# df_loaded = spark.read.format("mongodb") \
#     .option("spark.mongodb.read.connection.uri", mongo_url) \
#     .option("spark.mongodb.read.database", config["MONGODB_DATABASE_NAME"]) \
#     .option("spark.mongodb.read.collection", config["MONGODB_COLLECTION_NAME"]) \
#     .load()
# print("="*100)

# # 읽어온 데이터 출력
# print("10. MongoDB에서 데이터 읽기")
# df_loaded.show()
# print("="*100)

####################################################################################################
##################################     MongoDB Example      ########################################
####################################################################################################

####################################################################################################
##################################         MongoDB          ########################################
####################################################################################################


    
# MongoDB에서 데이터 읽기
print("9. MongoDB에서 데이터 읽기")


start_timer("MongoDB에서 데이터 읽기")
df_loaded = spark.read.format("mongodb") \
    .option("spark.mongodb.read.connection.uri", mongo_url) \
    .option("spark.mongodb.read.database", config["MONGODB_DATABASE_NAME"]) \
    .option("spark.mongodb.read.collection", "transport") \
    .load()
end_timer()
print("="*100)

# 읽어온 데이터 출력
print("10. MongoDB에서 데이터 읽기")
df_loaded.show()
print("="*100)
df_loaded.printSchema()
print("="*100)

def process_data(df):
    agg_df = df.groupBy("eqp_id").avg("weight")
    return agg_df
start_timer("(eqp_id)별 평균 weight을 계산")
result = process_data(df_loaded)
end_timer()

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

start_timer("11. 특성 선택 및 벡터 생성")
feature_columns = ['gps_lat', 'gps_lon', 'speed', 'move_distance', 'move_time']  # 예측에 사용할 특성
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
data = assembler.transform(df_loaded)
end_timer()

start_timer("12. 'weight'는 레이블로 사용")
data = data.withColumnRenamed("weight", "label")
end_timer()

start_timer("13. 데이터를 훈련 세트와 테스트 세트로 분할")
train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)
end_timer()

start_timer("14. 선형 회귀 모델을 학습")
lr = LinearRegression(featuresCol="features", labelCol="label")
model = lr.fit(train_data)
end_timer()

start_timer("15. 테스트 데이터를 사용하여 모델을 평가")
predictions = model.transform(test_data)
evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
end_timer()
print("Root Mean Squared Error (RMSE) on test data =", rmse)


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
