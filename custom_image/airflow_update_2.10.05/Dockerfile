FROM apache/airflow:2.10.5
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       gcc \ 
       g++ \
       libsasl2-dev \
       heimdal-dev \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install \
    apache-airflow-providers-apache-hdfs \
    apache-airflow-providers-apache-hive \
    pykrx \
    apache-airflow-providers-amazon[s3fs] 
RUN pip uninstall -y argparse
