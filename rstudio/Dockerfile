FROM rocker/rstudio:3.6.3

RUN apt-get update && apt-get install -y libxml2-dev libgit2-dev zlib1g-dev

RUN R -e 'install.packages(c("devtools", "sparklyr"), dependencies=TRUE)'
RUN R -e 'sparklyr::spark_install(version = "2.4.1", verbose = TRUE)'
RUN R -e 'devtools::install_github("r-spark/sparkavroudf")'

RUN apt install apt-transport-https ca-certificates wget dirmngr gnupg software-properties-common
RUN wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | apt-key add - && \
add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/ && \
apt update && \
apt install adoptopenjdk-8-hotspot

ENV JAVA_HOME /usr/lib/jvm/java-8-oracle


RUN mv /root/spark /opt/ && \
    chown -R rstudio:rstudio /opt/spark/ && \
    ln -s /opt/spark/ /home/rstudio/