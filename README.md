# Spark

## 환경 구축
Github Action을 이용해서,
깃 레파지토리에 push만하면 최신화된 코드로 

1) docker image build
2) dockerhub로 push
3) 내부 K8s 클러스터에 Spark Job Submit
  
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

    FROM python:3.8 # -> 기존은 args로 받아오는 형태였는데, 하드하게, 3.8버전으로 fix.

    COPY examples/src/main/python/spark_code/yusu/ /opt/spark/work-dir/

    RUN pip3 install -r /opt/spark/work-dir/requirements.txt

    ############################################################
    ```
    위 내용들 Dockerfile에 추가.


2) `bcpkix-jdk15on-1.70.jar`,`bcprov-jdk15on-1.70.jar`
이 두 파일을 mvnrepository에서 설치 한 후, Spark 메인 디렉토리의 `jars`로 이동하여 해결.

3) 문제점을 찾아볼려고 다양한 것들을 바꿔보면서 submit을 해보며 찾았는데, 모든 조건이 동일할 때, tag명을 `latest`를 쓴 경우에 파드 내부적으로 코드 경로를 파악하지 못하는 문제점이 발생하고, 그렇지 않은 임의의 tag명을 붙였을 때는 정상적으로 코드가 작동하였다.

    이를 해결하기위해서,다음과 같이 쉘 코드를 수정하였고

    

    ```
    # 임의의 UUID를 생성합. 이때, uuidgen 명령어의 결과에서 '-'를 제거하고 앞부분만 사용하여 짧게 생성.

    UUID=$(uuidgen | tr -d '-' | cut -c 1-8)

    # 기본 IMAGE_TAG에 UUID를 붙여, 최종 IMAGE_TAG를 구성.
    
    IMAGE_TAG="${UUID}"
    ```

    이 후, `build` -> `push` -> `job-submit` 과정에서 사용되는 이미지 tag가 중복되지않고 매번 다르게 설정이 되어 해결되었다.


    `uuidgen` 명령어가 시스템에 설치되어 있지 않은 경우,다음과 같은 명령어로 설치해야한다.
    ```
    sudo apt-get install uuid-runtime  # Debian/Ubuntu
    sudo yum install uuidgen           # CentOS/RHEL
    ```
