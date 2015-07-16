FROM ubuntu:latest

RUN apt-get update && apt-get install -y \
  git \
  python-pip

RUN git clone https://github.com/cphl/oicr-aws-reporting.git
RUN pip install boto
WORKDIR oicr-aws-reporting/
RUN mkdir reports
