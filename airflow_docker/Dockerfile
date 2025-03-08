#FROM airflow_ok:1.0
FROM apache/airflow:2.4.2

USER root
WORKDIR /opt
#COPY Python-3.11.8.tgz /opt
#RUN sed -i s@/archive.ubuntu.com/@/mirrors.aliyun.com/@g /etc/apt/sources.list && \
#sed -i s@/security.ubuntu.com/@/mirrors.aliyun.com/@g /etc/apt/sources.list  && \
#apt clean && \
#apt-get update -y && \
#    apt-get install -y build-essential
##apt install wget && wget https://www.python.org/ftp/python/3.11.8/Python-3.11.8.tgz && \
#RUN tar -zxf Python-3.11.8.tgz && cd Python-3.11.8 && ./configure --prefix=/usr/local/python3 && \
#     ./configure --enable-optimizations && make && make install && rm -rf /usr/bin/python3 && \
#     ln -s /usr/local/bin/python3.11 /usr/bin/python

RUN mkdir -p /root/.config

COPY config.yaml  /root/.config
COPY requirements.txt /opt
USER airflow
RUN pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple  --trusted-host pypi.tuna.tsinghua.edu.cn
RUN pip install apache-airflow -U -i https://pypi.tuna.tsinghua.edu.cn/simple  --trusted-host pypi.tuna.tsinghua.edu.cn

