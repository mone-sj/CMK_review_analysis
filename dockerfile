FROM ubuntu:18.04

# language setting
RUN apt-get update \
    && apt-get install -y build-essential \
    && apt-get install -y locales
RUN locale-gen ko_KR.UTF-8
ENV LC_ALL ko_KR.UTF-8

# install crontab
RUN apt-get install -y cron
ADD ./etc/cronjob.txt /data/cron.d


RUN touch /usr/sbin/entrypoint.sh
RUN chmod 777 /usr/sbin/entrypoint.sh

# create the log file
RUN touch /data/analysis.log

ENTRYPOINT ["/usr/sbin/entrypoint.sh"]

# python3.6
RUN apt-get install curl -y \
    && curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
RUN apt-get install -y python3.6-venv
RUN apt-get install python3-pip -y \
    && python3.6 -m pip install pip --upgrade \
    && python3.6 -m pip install wheel
RUN apt-get install git -y \
    && apt install git

# torch / gpu

# install requirements.txt / krwordrank / kobert
COPY ./requirements.txt /requirements.txt
RUN pip install -r requirements.txt \
    && pip install git+https://github.com/lovit/KR-WordRank.git \
    && pip install git+https://git@github.com/SKTBrain/KoBERT.git@master


SHELL [ "/bin/bash" ]
