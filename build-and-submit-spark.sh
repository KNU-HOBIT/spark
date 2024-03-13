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
# 현재 년월일과 시분초 정보를 사용하여 데이터 정보를 생성
DATA_INFO=$(date +"%Y-%m-%d.%H-%M-%S")

# 데이터 정보를 IMAGE_TAG로 사용
IMAGE_TAG="${DATA_INFO}"

SPARK_DIR=$(jq -r '.SPARK_DIR' $CONFIG_FILE)
LOCAL_DIR=$(jq -r '.LOCAL_DIR' $CONFIG_FILE)
PYSPARK_CODE_DIR=$(jq -r '.PYSPARK_CODE_DIR' $CONFIG_FILE)
PYSPARK_CODE_NAME=$(jq -r '.PYSPARK_CODE_NAME' $CONFIG_FILE)
DOCKERFILE_DIR=$(jq -r '.DOCKERFILE_DIR' $CONFIG_FILE)
DOCKERFILE_NAME=$(jq -r '.DOCKERFILE_NAME' $CONFIG_FILE)

TARGET_DIR="${SPARK_DIR}/${PYSPARK_CODE_DIR}"


# 완전한 이미지 경로 구성
FULL_IMAGE_PATH="${IMAGE_REPO_NAME}/spark-py:${IMAGE_TAG}"

# Docker 이미지 빌드 명령어 저장
BUILD_CMD="./bin/docker-image-tool.sh -r $IMAGE_REPO_NAME -t $IMAGE_TAG -p $DOCKERFILE_DIR/$DOCKERFILE_NAME build"

# Docker 이미지 푸시 명령어 저장
PUSH_CMD="./bin/docker-image-tool.sh -r $IMAGE_REPO_NAME -t $IMAGE_TAG -p $DOCKERFILE_DIR/$DOCKERFILE_NAME push"

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
# 빌드 명령어를 로그 파일에 기록
echo "Building Docker image with command: $BUILD_CMD"
echo "Building Docker image with command: $BUILD_CMD" >> build_log.txt

# 푸시 명령어를 로그 파일에 기록
echo "Pushing Docker image with command: $PUSH_CMD"
echo "Pushing Docker image with command: $PUSH_CMD" >> build_log.txt

# Spark submit 명령어를 로그 파일에 기록
echo "$SPARK_SUBMIT_CMD"
echo "$SPARK_SUBMIT_CMD" >> spark_submit_log.txt

# 인자로 --local이 전달되었는지 확인
if [[ "$1" == "--local" ]]; then
    #################################################################### 
    # 로컬일 때는 이 부분을 활성화

    # 대상 디렉토리 생성
    mkdir -p "$TARGET_DIR"

    # 대상 디렉토리 내부의 모든 파일 삭제
    rm -rf "$TARGET_DIR"/*

    # 현재 디렉토리의 모든 파일을 대상 디렉토리로 복사
    cp -r ./* "$TARGET_DIR"

    # 대상 디렉토리 내용 확인
    cd "$TARGET_DIR"
    ls
    #################################################################### 
fi

cd $SPARK_DIR
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