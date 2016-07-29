FROM ubuntu:latest

RUN apt-get update && apt-get install -y python python-pip build-essential

RUN pip install --upgrade pip
RUN pip install pyyaml logging apscheduler elasticsearch


VOLUME /opt/application
COPY index_managment.py /opt/application/manager.py
COPY fnmatch.py /opt/application/fnmatch.py

WORKDIR /opt/application/