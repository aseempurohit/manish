FROM ubuntu:16.04
MAINTAINER Anuyog Chauhan "anuyog.chauhan@aricent.com"
ENV http_proxy http://165.225.104.34:80
ENV https_proxy https://165.225.104.34:80
RUN echo "8.8.8.8" >> /etc/resolv.conf
RUN echo "8.8.4.4" >> /etc/resolv.conf
RUN apt-get update
RUN apt-get install -y python python-redis
RUN echo "break cache"
RUN mkdir /opt
COPY . /opt
WORKDIR /opt
ENV USE_REDIS=TRUE
ENV REDIS_SERVICE_HOST="172.17.0.1"
ENV REDIS_SERVICE_PORT=6379

CMD ["/usr/bin/python","MrServerENS.py","5000"]
