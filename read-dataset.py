from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
import os

spark = SparkSession.builder \
        .appName("test") \
        .config("spark.mongodb.read.connection.uri", "mongodb://155.230.34.51:32323/mongodb.collid") \
        .config("spark.mongodb.write.connection.uri", "mongodb://155.230.34.51:32323/mongodb.collid") \
        .getOrCreate() \

spark.sparkContext.setLogLevel('WARN')

print("="*100)
print("FILES IN THIS DIRECTORY")
print(os.listdir(os.getcwd()))
print("="*100)


# 원본 데이터 URL에서 보스턴 주택 가격 데이터 세트를 로드
print("3. 원본 데이터 URL에서 보스턴 주택 가격 데이터 세트를 로드")
data_url = "http://lib.stat.cmu.edu/datasets/boston"
raw_df = pd.read_csv(data_url, sep="\s+", skiprows=22, header=None)
data = np.hstack([raw_df.values[::2, :], raw_df.values[1::2, :2]])
target = raw_df.values[1::2, 2]

# 보스턴 주택 가격 데이터 세트의 컬럼 이름 정의
print("4. 보스턴 주택 가격 데이터 세트의 컬럼 이름 정의")
boston_columns = [
    "CRIM", "ZN", "INDUS", "CHAS", "NOX", "RM", "AGE", "DIS", "RAD",
    "TAX", "PTRATIO", "B", "LSTAT"
]

# 데이터와 타겟을 결합하여 pandas DataFrame 생성
boston_pdf = pd.DataFrame(data, columns=boston_columns)
boston_pdf['PRICE'] = target

# pandas DataFrame을 Spark DataFrame으로 변환
print("5. pandas DataFrame을 Spark DataFrame으로 변환")
boston_sdf = spark.createDataFrame(boston_pdf)
print("="*100)
print(boston_sdf.show(10))
print("="*100)

import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# 저장할 디렉토리 경로 설정
directory = './plots/'

# 디렉토리가 존재하지 않으면 생성
if not os.path.exists(directory):
    os.makedirs(directory)

print("6. 시본의 regplot을 이용해 산점도와 선형 회귀 직선을 함께 표현")
print("="*100)
# 2개의 행과 4개의 열을 가진 subplots를 이용. axs는 4x2개의 ax를 가짐.
fig, axs = plt.subplots(figsize=(16,8) , ncols=4 , nrows=2)
lm_features = ['RM','ZN','INDUS','NOX','AGE','PTRATIO','LSTAT','RAD']
colors = ['g', 'r', 'b', 'c', 'm', 'y', 'orange', 'darkblue' ]
for i , feature in enumerate(lm_features):
    row = int(i/4)
    col = i%4
    # 시본의 regplot을 이용해 산점도와 선형 회귀 직선을 함께 표현
    sns.regplot(x=feature , y='PRICE',data=boston_pdf , ax=axs[row][col], color=colors[i])
    
# 지정된 파일 이름으로 현재 그림을 저장
timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
filename = f'PRICE 산점도 & 선형 회귀 직선-{timestamp}.png'
plt.savefig(os.path.join(directory, filename))
print("="*100)


# MONGODB TEST
from pyspark.sql import Row

# 예제 데이터 생성
print("7. MONGODB TEST / 예제 데이터 생성")
print("="*100)
data = [Row(name="Alice", age=25), Row(name="Bob", age=30)]
df = spark.createDataFrame(data)
print(df.show(10))
print("="*100)

# MongoDB에 데이터 쓰기
print("8. MongoDB에 데이터 쓰기")
df.write.format("mongo").mode("append").save()

print("="*100)

# MongoDB에서 데이터 읽기
print("9. MongoDB에서 데이터 읽기")
df_loaded = spark.read.format("mongo").load()
print("="*100)

# 읽어온 데이터 출력
print("10. MongoDB에서 데이터 읽기")
df_loaded.show()
print("="*100)