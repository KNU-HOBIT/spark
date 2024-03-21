#!/bin/bash

# 완전한 이미지 경로 인자로부터 받기
FULL_IMAGE_PATH=$1

# config.json 파일에서 설정값을 읽어옵니다.
CONFIG_FILE="./config.json"
PYSPARK_CODE_NAME=$(jq -r '.PYSPARK_CODE_NAME' $CONFIG_FILE)

# Spark job 제출 명령어를 변수에 저장
SPARK_SUBMIT_CMD="
spark-submit \
  $PYSPARK_CODE_NAME --config $CONFIG_FILE --mode cluster --image $FULL_IMAGE_PATH
"

echo "============================================================"
echo "실제 Spark job 제출"
echo "$SPARK_SUBMIT_CMD"
echo "============================================================"
eval $SPARK_SUBMIT_CMD