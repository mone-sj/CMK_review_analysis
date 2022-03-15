ARG CUDA_DOCKER_VERSION=10.0-devel-ubuntu18.04
FROM nvidia/cuda:${CUDA_DOCKER_VERSION}
#FROM ubuntu:18.04

# language setting
RUN apt-get update \
    && apt-get install -y build-essential \
    && apt-get install -y locales
RUN locale-gen ko_KR.UTF-8
ENV LC_ALL ko_KR.UTF-8

# install cron
RUN apt-get install cron -y

# create log file
RUN touch /var/log/cron_naver.log
RUN touch /var/log/cron_glowpick.log

# python3.6
RUN apt-get install curl -y \
    && curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
RUN apt-get update
RUN apt-get install -y python3.6-venv
RUN apt-get install python3-pip -y \
    && python3.6 -m pip install pip --upgrade \
    && python3.6 -m pip install wheel
RUN apt-get install git -y \
    && apt install git
RUN apt-get install vim -y

### time setting UTC -> KST
ARG DEBIAN_FRONTEND=noninterative
ENV TZ=Asia/Seoul
RUN apt-get install -y tzdata
RUN ln -sf /usr/local/lib/python3.6/dist-packages/pytz/zoneinfo/Asia/Seoul /etc/localtime

# torch v1.7.1 / gpu
RUN pip install torch==1.7.1+cu110 torchvision==0.8.2+cu110 torchaudio==0.7.2 -f https://download.pytorch.org/whl/torch_stable.html

COPY ./requirements.txt /requirements.txt
RUN pip install -r requirements.txt \
    && pip install git+https://github.com/lovit/KR-WordRank.git \
    && pip install git+https://git@github.com/SKTBrain/KoBERT.git@master

SHELL [ "/bin/bash" ]
