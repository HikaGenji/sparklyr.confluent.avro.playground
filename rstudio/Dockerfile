FROM rocker/rstudio:3.6.3

RUN apt-get update && apt-get install -y libxml2-dev libgit2-dev zlib1g-dev

RUN R -e 'install.packages("sparklyr")'
RUN R -e 'install.packages("devtools", dependencies=TRUE)'
RUN R -e 'devtools::install_github("hikagenji/sparkavroudf")'
RUN wget https://archive.apache.org/dist/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz &&\
tar xvf spark-2.4.5-bin-hadoop2.7.tgz && \
mv spark-2.4.5-bin-hadoop2.7/ /opt/spark 

ENV SPARK_HOME=/opt/spark

RUN apt install -y apt-transport-https ca-certificates wget dirmngr gnupg software-properties-common
RUN wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | apt-key add - && \
add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/ && \
apt update && \
apt install -y adoptopenjdk-8-hotspot

ENV JAVA_HOME /usr/lib/jvm/java-8-oracle


RUN chown -R rstudio:rstudio /opt/spark/ && \
    ln -s /opt/spark/ /home/rstudio/