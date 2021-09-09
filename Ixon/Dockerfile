FROM beerepo.tech.beegroup-cimne.com:5000/mr-docker
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update
RUN apt-get install -y python3 python3-pip openvpn bridge-utils libssl-dev openssl

ADD requirements.txt requirements.txt

RUN pip install -r requirements.txt

USER ubuntu

CMD ["python3"]