#!/bin/bash

# 완전한 이미지 경로 인자로부터 받기
FULL_IMAGE_PATH=$1

# config.json 파일에서 설정값을 읽어옵니다.
CONFIG_FILE="./config.json"
SPARK_HOME=$(jq -r '.SPARK_HOME' $CONFIG_FILE)
K8S_CLUSTER_ADDRESS=$(jq -r '.K8S_CLUSTER_ADDRESS' $CONFIG_FILE)
SPARK_JOB_NAME=$(jq -r '.SPARK_JOB_NAME' $CONFIG_FILE)
NUM_EXECUTORS=$(jq -r '.NUM_EXECUTORS' $CONFIG_FILE)
EXECUTOR_CORES=$(jq -r '.EXECUTOR_CORES' $CONFIG_FILE)
EXECUTOR_MEMORY=$(jq -r '.EXECUTOR_MEMORY' $CONFIG_FILE)
SERVICEACCOUNT_NAME=$(jq -r '.SERVICEACCOUNT_NAME' $CONFIG_FILE)
PYSPARK_CODE_NAME=$(jq -r '.PYSPARK_CODE_NAME' $CONFIG_FILE)

# # Docker 이미지 빌드 명령어 저장
BUILD_CMD="docker build -t $FULL_IMAGE_PATH ."

# Docker 이미지 푸시 명령어 저장
PUSH_CMD="docker push $FULL_IMAGE_PATH"


# Spark job 제출 명령어를 변수에 저장
SPARK_SUBMIT_CMD="
$SPARK_HOME/bin/spark-submit \
  --master k8s://$K8S_CLUSTER_ADDRESS \
  --name $SPARK_JOB_NAME \
  --deploy-mode cluster \
  --num-executors $NUM_EXECUTORS \
  --executor-cores $EXECUTOR_CORES \
  --executor-memory $EXECUTOR_MEMORY \
  --conf spark.kubernetes.container.image=$FULL_IMAGE_PATH \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=$SERVICEACCOUNT_NAME \
  --jars https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.2.2/mongo-spark-connector_2.12-10.2.2.jar, \
        https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.8.2/mongodb-driver-sync-4.8.2.jar, \
        https://repo1.maven.org/maven2/org/mongodb/bson/4.8.2/bson-4.8.2.jar, \
        https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/4.8.2/mongodb-driver-core-4.8.2.jar, \
        https://repo1.maven.org/maven2/org/mongodb/bson-record-codec/4.8.2/bson-record-codec-4.8.2.jar \
  local:///workspace/pyspark/$PYSPARK_CODE_NAME
"

echo "============================================================"
echo "실제 Spark job 제출"
echo "$SPARK_SUBMIT_CMD"
echo "============================================================"
eval $SPARK_SUBMIT_CMD