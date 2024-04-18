FROM noyusu/pyspark:3.2.4

USER root

WORKDIR /workspace/pyspark

COPY . /workspace/pyspark

RUN pip3 install --no-cache-dir -r requirements.txt