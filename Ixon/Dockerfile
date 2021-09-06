FROM python:3.8

WORKDIR /Ixon

COPY requirements.txt .
RUN pip install -r requirements.txt

RUN mkdir /logs
RUN mkdir /out

COPY config.json .
COPY Ixon.py .
COPY ixon_mrjob.py .
COPY logger.py .
COPY manage_credentials.py .