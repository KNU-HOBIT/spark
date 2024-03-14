#!/bin/bash

# config.json 파일에서 설정값을 읽어옵니다.
CONFIG_FILE="./config.json"
SPARK_HOME==$(jq -r '.SPARK_HOME' $CONFIG_FILE)
SPARK_JOB_NAME=$(jq -r '.SPARK_JOB_NAME' $CONFIG_FILE)
K8S_CLUSTER_ADDRESS=$(jq -r '.K8S_CLUSTER_ADDRESS' $CONFIG_FILE)
NUM_EXECUTORS=$(jq -r '.NUM_EXECUTORS' $CONFIG_FILE)
EXECUTOR_CORES=$(jq -r '.EXECUTOR_CORES' $CONFIG_FILE)
EXECUTOR_MEMORY=$(jq -r '.EXECUTOR_MEMORY' $CONFIG_FILE)
SERVICEACCOUNT_NAME=$(jq -r '.SERVICEACCOUNT_NAME' $CONFIG_FILE)
IMAGE_REPO_NAME=$(jq -r '.IMAGE_REPO_NAME' $CONFIG_FILE)
# 현재 년월일과 시분초 정보를 사용하여 데이터 정보를 생성
DATA_INFO=$(date +"%Y-%m-%d.%H-%M-%S")
# 데이터 정보를 IMAGE_TAG로 사용
IMAGE_TAG="${DATA_INFO}"

PYSPARK_CODE_NAME=$(jq -r '.PYSPARK_CODE_NAME' $CONFIG_FILE)

# 완전한 이미지 경로 구성
FULL_IMAGE_PATH="${IMAGE_REPO_NAME}/pyspark:${IMAGE_TAG}"

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
  local:///workspace/pyspark/$PYSPARK_CODE_NAME
"

echo "============================================================"
echo "빌드 명령어"
echo "$BUILD_CMD"
echo "============================================================"
echo "푸시 명령어"
echo "$PUSH_CMD"
echo "============================================================"
echo "Spark submit 명령어"
echo "$SPARK_SUBMIT_CMD"
echo "============================================================"
echo "============================================================"
echo "빌드 명령어 실행"
echo "============================================================"
eval $BUILD_CMD
echo "============================================================"
echo "푸시 명령어 실행"
echo "============================================================"
eval $PUSH_CMD
echo "============================================================"
echo "실제 Spark job 제출"
echo "============================================================"
eval $SPARK_SUBMIT_CMD