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
# 임의의 UUID를 생성합니다. 이때, uuidgen 명령어의 결과에서 '-'를 제거하고 앞부분만 사용하여 짧게 만듭니다.
UUID=$(uuidgen | tr -d '-' | cut -c 1-8)
# 기본 IMAGE_TAG에 UUID를 붙여서 최종 IMAGE_TAG를 구성합니다.
IMAGE_TAG="${UUID}"

SPARK_DIR=$(jq -r '.SPARK_DIR' $CONFIG_FILE)
LOCAL_DIR=$(jq -r '.LOCAL_DIR' $CONFIG_FILE)
PYSPARK_CODE_DIR=$(jq -r '.PYSPARK_CODE_DIR' $CONFIG_FILE)
PYSPARK_CODE_NAME=$(jq -r '.PYSPARK_CODE_NAME' $CONFIG_FILE)
DOCKERFILE_DIR=$(jq -r '.DOCKERFILE_DIR' $CONFIG_FILE)
DOCKERFILE_NAME=$(jq -r '.DOCKERFILE_NAME' $CONFIG_FILE)

# 완전한 이미지 경로 구성
FULL_IMAGE_PATH="${IMAGE_REPO_NAME}/${IMAGE_NAME}:${IMAGE_TAG}"

cd $SPARK_DIR

# Docker 이미지 빌드 명령어 저장
BUILD_CMD="./bin/docker-image-tool.sh -r $IMAGE_REPO_NAME -t $IMAGE_TAG -p $DOCKERFILE_DIR/$DOCKERFILE_NAME build"

# Docker 이미지 푸시 명령어 저장
PUSH_CMD="./bin/docker-image-tool.sh -r $IMAGE_REPO_NAME -t $IMAGE_TAG -p $DOCKERFILE_DIR/$DOCKERFILE_NAME push"

# 빌드 명령어 디버깅
echo "Building Docker image with command: $BUILD_CMD"
# 빌드 명령어 실행
eval $BUILD_CMD

# 푸시 명령어 디버깅
echo "Pushing Docker image with command: $PUSH_CMD"
# 푸시 명령어 실행
eval $PUSH_CMD

# Spark job 제출 명령어를 변수에 저장
SPARK_SUBMIT_CMD="
./bin/spark-submit \
  --master k8s://$K8S_CLUSTER_ADDRESS \
  --name $SPARK_JOB_NAME \
  --deploy-mode cluster \
  --num-executors $NUM_EXECUTORS \
  --executor-cores $EXECUTOR_CORES \
  --executor-memory $EXECUTOR_MEMORY \
  --conf spark.kubernetes.namespace=$NAMESPACE \
  --conf spark.kubernetes.container.image=$FULL_IMAGE_PATH \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=$SERVICEACCOUNT_NAME \
  --conf spark.driver.extraJavaOptions='-Divy.cache.dir=/tmp -Divy.home=/tmp' \
  --conf spark.sql.streaming.kafka.useDeprecatedOffsetFetching=true \
  local://$LOCAL_DIR/$PYSPARK_CODE_DIR/$PYSPARK_CODE_NAME
"

# 명령어를 디버깅(출력) 목적으로 Echo
echo "$SPARK_SUBMIT_CMD"

# 실제 Spark job 제출
eval $SPARK_SUBMIT_CMD