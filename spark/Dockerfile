FROM apache/spark:4.0.0

USER root

RUN pip install --no-cache-dir pyspark==4.0.0 psycopg2-binary==2.9.13

WORKDIR /opt/spark/work-dir

USER spark