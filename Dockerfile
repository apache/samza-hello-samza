FROM openjdk:8

RUN apt-get update
RUN apt install -y netcat
RUN apt install -y vim
RUN apt install -y net-tools

WORKDIR /opt
RUN wget http://www.gtlib.gatech.edu/pub/apache/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz
RUN tar -xvzf apache-maven-3.6.3-bin.tar.gz
ENV M2_HOME="/opt/apache-maven-3.6.3"
ENV PATH="$M2_HOME/bin:${PATH}"

WORKDIR /usr/src/app
RUN git clone https://github.com/mpfeiffer00/samza-hello-samza/ samza

WORKDIR /usr/src/app/samza
RUN ./bin/grid bootstrap
RUN ./bin/deploy.sh
