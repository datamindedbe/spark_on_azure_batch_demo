FROM python:3.6

# Install OpenJDK 8, and monitoring tooling
RUN \
  apt-get update && \
  apt-get install -y openjdk-8-jdk htop bmon && \
  rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade setuptools
COPY requirements.txt /
RUN pip install -r /requirements.txt

ENV PATH=$PATH:/src
ENV PYTHONPATH /src

ADD ./ /src
WORKDIR /src/


