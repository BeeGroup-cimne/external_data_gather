FROM python:3.8
RUN apt-get update && apt-get install -y build-essential libsasl2-dev

WORKDIR /ixon_mr/

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY ixon_mrjob.py ixon_mrjob.py
COPY ixon_gather.py ixon_gather.py
COPY logger.py logger.py
COPY Ixon.py Ixon.py
COPY utils.py utils.py

RUN mkdir "vpn_files"