

# 전체 설정
export PYSPARK_PYTHON='/home/scouter/yousoo/spark/myenv/bin/python'

# 드라이버, 워커 개별 설정
export PYSPARK_DRIVER_PYTHON='/home/scouter/yousoo/spark/myenv/bin/python'
export PYSPARK_WORKER_PYTHON='/home/scouter/yousoo/spark/myenv/bin/python'

spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.2 read-dataset.py