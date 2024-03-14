FROM apache/spark:3.4.1-python3

USER root

WORKDIR /workspace/pyspark

COPY . /workspace/pyspark

RUN pip3 install --no-cache-dir -r requirements.txt

USER spark