#!/bin/bash

# config.json 파일에서 설정값을 읽어옵니다.
CONFIG_FILE="./config.json"

SPARK_JOB_NAME=$(jq -r '.SPARK_JOB_NAME' $CONFIG_FILE)
K8S_CLUSTER_ADDRESS=$(jq -r '.K8S_CLUSTER_ADDRESS' $CONFIG_FILE)
NUM_EXECUTORS=$(jq -r '.NUM_EXECUTORS' $CONFIG_FILE)
EXECUTOR_CORES=$(jq -r '.EXECUTOR_CORES' $CONFIG_FILE)
EXECUTOR_MEMORY=$(jq -r '.EXECUTOR_MEMORY' $CONFIG_FILE)
SERVICEACCOUNT_NAME=$(jq -r '.SERVICEACCOUNT_NAME' $CONFIG_FILE)
NAMESPACE=$(jq -r '.NAMESPACE' $CONFIG_FILE)
IMAGE_REPO_NAME=$(jq -r '.IMAGE_REPO_NAME' $CONFIG_FILE)
IMAGE_NAME=$(jq -r '.IMAGE_NAME' $CONFIG_FILE)
IMAGE_TAG=$(jq -r '.IMAGE_TAG' $CONFIG_FILE)

SPARK_DIR=$(jq -r '.SPARK_DIR' $CONFIG_FILE)
LOCAL_DIR=$(jq -r '.LOCAL_DIR' $CONFIG_FILE)
PYSPARK_CODE_DIR=$(jq -r '.PYSPARK_CODE_DIR' $CONFIG_FILE)
PYSPARK_CODE_NAME=$(jq -r '.PYSPARK_CODE_NAME' $CONFIG_FILE)
DOCKERFILE_DIR=$(jq -r '.DOCKERFILE_DIR' $CONFIG_FILE)
DOCKERFILE_NAME=$(jq -r '.DOCKERFILE_NAME' $CONFIG_FILE)

# 완전한 이미지 경로 구성
FULL_IMAGE_PATH="${IMAGE_REPO_NAME}/${IMAGE_NAME}:${IMAGE_TAG}"

# Docker 이미지를 빌드하고 푸시
$SPARK_DIR/bin/docker-image-tool.sh -r $IMAGE_REPO_NAME -t $IMAGE_TAG -p $DOCKERFILE_DIR/$DOCKERFILE_NAME build
$SPARK_DIR/bin/docker-image-tool.sh -r $IMAGE_REPO_NAME -t $IMAGE_TAG -p $DOCKERFILE_DIR/$DOCKERFILE_NAME push

# Spark job 제출 명령어를 변수에 저장
SPARK_SUBMIT_CMD="
$SPARK_DIR/bin/spark-submit \
  --master k8s://$K8S_CLUSTER_ADDRESS \
  --deploy-mode cluster \
  --name $SPARK_JOB_NAME \
  --num-executors $NUM_EXECUTORS \
  --executor-cores $EXECUTOR_CORES \
  --executor-memory $EXECUTOR_MEMORY \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=$SERVICEACCOUNT_NAME \
  --conf spark.sql.streaming.kafka.useDeprecatedOffsetFetching=true \
  --conf spark.kubernetes.namespace=$NAMESPACE \
  --conf spark.kubernetes.container.image=$FULL_IMAGE_PATH \
  --conf spark.driver.extraJavaOptions='-Divy.cache.dir=/tmp -Divy.home=/tmp' \
  local://$LOCAL_DIR/$PYSPARK_CODE_NAME
"

# 명령어를 디버깅(출력) 목적으로 Echo
echo "$SPARK_SUBMIT_CMD"

# 실제 Spark job 제출
eval $SPARK_SUBMIT_CMD