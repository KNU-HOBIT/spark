

# 전체 설정
export PYSPARK_PYTHON='/home/scouter/yousoo/spark/spark_env/bin/python'

# 드라이버, 워커 개별 설정
export PYSPARK_DRIVER_PYTHON='/home/scouter/yousoo/spark/spark_env/bin/python'
export PYSPARK_WORKER_PYTHON='/home/scouter/yousoo/spark/spark_env/bin/python'

spark-submit read-dataset.py