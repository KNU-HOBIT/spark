# config.json 파일에서 설정값을 읽어옵니다.
CONFIG_FILE="./config.json"
LOCAL_ENV_PYTHON=$(jq -r '.LOCAL_ENV_PYTHON' $CONFIG_FILE)

# 전체 설정, 드라이버, 워커 개별 설정
export PYSPARK_PYTHON="$LOCAL_ENV_PYTHON"
export PYSPARK_DRIVER_PYTHON="$LOCAL_ENV_PYTHON"
export PYSPARK_WORKER_PYTHON="$LOCAL_ENV_PYTHON"

# JAR_URLS 배열 읽기 & 배열을 쉼표로 구분된 문자열로 변환
readarray -t JAR_URLS < <(jq -r '.JAR_URLS[]' $CONFIG_FILE)
JARS=$(IFS=,; echo "${JAR_URLS[*]}")

spark-submit \
    --jars $JARS \
    read-dataset.py --config $CONFIG_FILE --mode local


# --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.2
# 를 사용했지만, 이 옵션은 위의 라이브러리 + 종속 라이브러리들을 드라이버 노드에만, 자동으로 다운로드함.
#
# 하지만, 워커노드에는 위 라이브러리들이 할당되지 않아, 코드가 cluster모드로 k8s에 제출되어도 작동하지않는 문제점이 있었음
# --jars <URL1>,<URL2>,<URL3>,... 를 이용하여 메이블 레포 링크를 걸어 마스터노드+워커노드 전부 jar 라이브러리를 할당할 수 있다.