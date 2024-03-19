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

# JAR_URLS 배열 읽기 & 배열을 쉼표로 구분된 문자열로 변환
readarray -t JAR_URLS < <(jq -r '.JAR_URLS[]' $CONFIG_FILE)
JARS=$(IFS=,; echo "${JAR_URLS[*]}")

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
  --jars $JARS \
  local:///workspace/pyspark/$PYSPARK_CODE_NAME --config $CONFIG_FILE --mode cluster
"

# --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.2
# 를 사용했지만, 이 옵션은 위의 라이브러리 + 종속 라이브러리들을 드라이버 노드에만, 자동으로 다운로드함.
#
# 하지만, 워커노드에는 위 라이브러리들이 할당되지 않아, 코드가 cluster모드로 k8s에 제출되어도 작동하지않는 문제점이 있었음
# --jars <URL1>,<URL2>,<URL3>,... 를 이용하여 메이블 레포 링크를 걸어 마스터노드+워커노드 전부 jar 라이브러리를 할당할 수 있다.

echo "============================================================"
echo "실제 Spark job 제출"
echo "$SPARK_SUBMIT_CMD"
echo "============================================================"
eval $SPARK_SUBMIT_CMD