FROM apache/airflow:2.5.1

ARG AIRFLOW_VERSION=2.5.1
ARG AIRFLOW_HOME=/usr/local/airflow
ARG AIRFLOW_DEPS=""
ARG PYTHON_DEPS=""
ARG SPARK_VERSION="3.1.2"
ARG HADOOP_VERSION="3.2"
ENV AIRFLOW_GPL_UNIDECODE yes
ENV SPARK_HOME /usr/local/spark

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

#USER root
#RUN set -ex \
#    && buildDeps=' \
#        freetds-dev \
#        libkrb5-dev \
#        libsasl2-dev \
#        libssl-dev \
#        libffi-dev \
#        libpq-dev \
#        git \
#    ' \
#    && apt-get update -yqq \
#    && apt-get upgrade -yqq \
#    && apt-get install -yqq --no-install-recommends \
#        $buildDeps \
#        freetds-bin \
#        build-essential \
#        default-libmysqlclient-dev \
#        apt-utils \
#        curl \
#        rsync \
#        netcat \
#        locales \
#        iputils-ping \
#        telnet \
#    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
#    && locale-gen \
#    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
#    && useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow \
#    && pip install -U pip setuptools wheel \
#    && pip install pytz \
#    && pip install pyOpenSSL \
#    && pip install ndg-httpsclient \
#    && pip install pyasn1 \
#    && pip install -r requirements.txt \
#    && pip install --use-feature=2020-resolver apache-airflow[crypto,celery,postgres,hive,jdbc,mysql,ssh${AIRFLOW_DEPS:+,}${AIRFLOW_DEPS}]==${AIRFLOW_VERSION} \
#    && pip install 'redis>=2.10.5,<3' \
#    && if [ -n "${PYTHON_DEPS}" ]; then pip install ${PYTHON_DEPS}; fi \
#    && apt-get purge --auto-remove -yqq $buildDeps \
#    && apt-get autoremove -yqq --purge \
#    && apt-get clean \
#    && rm -rf \
#        /var/lib/apt/lists/* \
#        /tmp/* \
#        /var/tmp/* \
#        /usr/share/man \
#        /usr/share/doc \
#        /usr/share/doc-base \
#    && python --version \
#    && pip freeze \

###############################
## Begin JAVA installation
###############################
# Java is required in order to spark-submit work
# Install OpenJDK-8
#RUN apt-get update && \
#    apt-get install -y software-properties-common && \
#    apt-get install -y gnupg2 && \
#    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys EB9B1D8886F44E2A && \
#    add-apt-repository "deb http://security.debian.org/debian-security stretch/updates main" && \
#    apt-get update && \
#    apt-get install -y openjdk-8-jdk && \
#    pip freeze && \
#    java -version $$ \
#    javac -version

###
USER root
RUN apt-get update && \
  apt-get install -y default-jdk

# Setup JAVA_HOME
#ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
RUN export JAVA_HOME

ENV PATH $PATH:$JAVA_HOME/bin
RUN export PATH
###############################
## Finish JAVA installation
###############################
