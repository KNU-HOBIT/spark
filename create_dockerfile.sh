#!/bin/bash

# config.json 파일에서 설정값을 읽어옵니다.
CONFIG_FILE="./config.json"


PYSPARK_CODE_DIR=$(jq -r '.PYSPARK_CODE_DIR' $CONFIG_FILE)
DOCKERFILE_DIR=$(jq -r '.DOCKERFILE_DIR' $CONFIG_FILE)
DOCKERFILE_NAME=$(jq -r '.DOCKERFILE_NAME' $CONFIG_FILE)
LOCAL_DIR=$(jq -r '.LOCAL_DIR' $CONFIG_FILE)

# Dockerfile 생성 시작
{
echo '
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

ARG base_img

FROM $base_img
WORKDIR /

# Reset to root to run installation tasks
USER 0

RUN mkdir ${SPARK_HOME}/python
RUN apt-get update && \
    apt install -y python3 python3-pip && \
    pip3 install --upgrade pip setuptools && \
    # Removed the .cache to save space
    rm -rf /root/.cache && rm -rf /var/cache/apt/* && rm -rf /var/lib/apt/lists/*

COPY python/pyspark ${SPARK_HOME}/python/pyspark
COPY python/lib ${SPARK_HOME}/python/lib
'
# 중간 부분 동적 생성
echo "# 해당 디렉토리의 모든 파일을 컨테이너의 작업 디렉토리로 복사"
echo "COPY $PYSPARK_CODE_DIR $LOCAL_DIR/$PYSPARK_CODE_DIR"
echo "COPY $PYSPARK_CODE_DIR $LOCAL_DIR/work-dir"
echo ""
echo "# Install Python dependencies from requirements.txt"
echo "RUN pip3 install -r $LOCAL_DIR/$PYSPARK_CODE_DIR/requirements.txt"
echo ""
echo "WORKDIR $LOCAL_DIR/work-dir"
echo ""

# Dockerfile 마무리 부분
echo '
ENTRYPOINT [ "/opt/entrypoint.sh" ]

# Specify the User that the actual main process will run as
ARG spark_uid=185
USER ${spark_uid}
'

} > $DOCKERFILE_DIR/$DOCKERFILE_NAME

# Dockerfile 내용 디버깅
echo "Dockerfile has been created with the following contents:"
cat $DOCKERFILE_DIR/$DOCKERFILE_NAME