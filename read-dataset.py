from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
import os
from sklearn.datasets import fetch_openml


spark = SparkSession.builder \
        .master("local") \
        .appName("newnewdaddy") \
        .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

# csv dataset 경로
print("csv dataset 경로")
data_path = os.path.join(os.getcwd(), 'test.csv')

print("="*100)
print("FILES IN THIS DIRECTORY")
print(os.listdir(os.getcwd()))
print("="*100)

# 1. pandas dataframe 생성 및 출력
print("1. pandas dataframe 생성 및 출력")
pdf = pd.read_csv(data_path)
print("="*100)
print("PANDAS DATAFRAME")
print(pdf.head(10))
print("="*100)

# 2. spark dataframe 생성 및 출력
print("2. spark dataframe 생성 및 출력")
sdf = spark.read.csv(data_path, header=True)
print("="*100)
print("SPARK DATAFRAME")
print(sdf.show(10))
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