from pyspark.sql import SparkSession
import pandas as pd
import os

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