# Spark on K8s (deprecated)

## 환경 구축
Github Action을 이용해서,
깃 레파지토리에 push만하면 최신화된 코드로 

1) docker image build
2) dockerhub로 push
3) 내부 K8s 클러스터에 Spark Job Submit
4) 로컬에서 submit한 결과와 K8s에 submit한 결과가 다름.
  
까지 자동화 시킨
개발 Pipeline 구성이 목표.

### 환경 구축 중 발생한 이슈사항.

1) 종속 라이브러리 설치와 관련하여, 실행 오류발생.
2) 인증 관련 오류 발생.(KubernetesClientException("JcaPEMKeyConverter is provided by BouncyCastle, an optional dependency. To use support for EC Keys you must explicitly add this dependency to classpath.");)
3) driver pod 내에서 코드의 경로를 찾지 못하는 문제점 발생.

수정사항.

1) `requirements.txt` 를 통해 파이썬 모듈 관리하도록 수정 &
    해당 모듈들 도커 이미지에 적용시키기 위해
    `kubernetes/dockerfiles/spark/bindings/python/Dockerfile` 수정.
    ```
    ######################## 수정 ##############################

    COPY examples/src/main/python/spark_code/yusu/ /opt/spark/work-dir/

    RUN pip3 install -r /opt/spark/work-dir/requirements.txt

    ############################################################
    ```
    위 내용들 Dockerfile에 추가. 원본 도커파일은 [다음](https://github.com/apache/spark/blob/master/resource-managers/kubernetes/docker/src/main/dockerfiles/spark/bindings/python/Dockerfile)을 참고.


2) `bcpkix-jdk15on-1.70.jar`,`bcprov-jdk15on-1.70.jar`
이 두 파일을 mvnrepository에서 설치 한 후, Spark 메인 디렉토리의 `jars`로 이동하여 해결.

1) 문제점을 찾아볼려고 다양한 것들을 바꿔보면서 submit을 해보며 찾았는데, 모든 조건이 동일할 때, tag명을 `latest`를 쓴 경우에 파드 내부적으로 코드 경로를 파악하지 못하는 문제점이 발생하고, 그렇지 않은 임의의 tag명을 붙였을 때는 정상적으로 코드가 작동하였다.

    이를 해결하기위해서,다음과 같이 쉘 코드를 수정

    ```
    # 현재 년월일과 시분초 정보를 사용하여 데이터 정보를 생성
    DATA_INFO=$(date +"%Y-%m-%d.%H-%M-%S")

    # 데이터 정보를 IMAGE_TAG로 사용
    IMAGE_TAG="${DATA_INFO}"

    . . .

    # 완전한 이미지 경로 구성
    FULL_IMAGE_PATH="${IMAGE_REPO_NAME}/spark-py:${IMAGE_TAG}"
    ```

    이 후, `build` -> `push` -> `job-submit` 과정에서 사용되는 이미지 tag가 중복되지않고 매번 다르게 설정이 되어 해결되었다.


2) `kubernetes/dockerfiles/spark/bindings/python/Dockerfile` 을 수정

    ```
    ######################## 수정 ##############################

     RUN mkdir ${SPARK_HOME}/python
     RUN apt-get update && \
     apt install -y python3 python3-pip && \
     pip3 install --upgrade pip setuptools && \
     pip install pyspark==3.2.4 && \
     # Removed the .cache to save space
     rm -r /root/.cache && rm -rf /var/cache/apt/*

    ############################################################
    ```
    위 내용로 Dockerfile 수정. 원본 도커파일은 [다음](https://github.com/apache/spark/blob/master/resource-managers/kubernetes/docker/src/main/dockerfiles/spark/bindings/python/Dockerfile)을 참고.

    로컬도 마찬가지로 다음과 같은 명령어로 가상환경 생성.

   1. 필요한 패키지 설치
        ```
        sudo apt update
        
        sudo apt install -y build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev wget
        ```
   2. Python 3.10 소스코드 다운로드
        ```
        wget https://www.python.org/ftp/python/3.10.12/Python-3.10.12.tgz
        ```
   3. 압축 해제
        ```
        tar -xf Python-3.10.12.tgz
        cd Python-3.10.12
        ```
   4. 소스 코드 컴파일
        ```
        ./configure --enable-optimizations
        make -j$(nproc)
        sudo make altinstall
        ```
   5. 가상환경 생성
        ```
        python3.10 -m venv myenv
        ```

3) `SPARK_HOME/kubernetes/dockerfiles/spark/entrypoint.sh`의 내용 수정.
     ```
     case "$1" in
     driver)
     shift 1
     CMD=(
          "$SPARK_HOME/bin/spark-submit"
          --conf "spark.driver.bindAddress=$SPARK_DRIVER_BIND_ADDRESS"
          --deploy-mode cluster
          "$@"
     )
     ;;
     ```
     디플로이모드가 client로 고정되어있는 것을 수정

## HOW to DEPLOY


### 로컬 모드(개발)
> 현재 로컬 환경, 단일 서버에서 실행.

`local-submit.sh`에서 다음과 같은 명령어로 작동.

```
spark-submit \
    --jars $JARS \
    read-dataset.py \
    --config $CONFIG_FILE \
    --mode local
```
- `CONFIG_FILE`에는 `config.json`의 경로. 
- `JARS`에는 maven.repo에서 제공하는 JAR 라이브러리 다운로드 링크 List. 



