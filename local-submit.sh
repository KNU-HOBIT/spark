MY_PYTHON="/home/scouter/anaconda3/envs/myenv/bin/python"
# 전체 설정
export PYSPARK_PYTHON="$MY_PYTHON"

# 드라이버, 워커 개별 설정
export PYSPARK_DRIVER_PYTHON="$MY_PYTHON"
export PYSPARK_WORKER_PYTHON="$MY_PYTHON"

spark-submit \
    --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.2 \
    read-dataset.py