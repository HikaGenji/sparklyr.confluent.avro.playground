FROM rocker/rstudio:3.6.3

RUN apt-get update && apt-get install -y libxml2-dev libgit2-dev zlib1g-dev
RUN apt-get update && apt-get install -y scala

RUN apt install -y apt-transport-https ca-certificates wget dirmngr gnupg software-properties-common
RUN wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | apt-key add - && \
add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/ && \
apt update && \
apt install -y adoptopenjdk-8-hotspot

RUN R -e 'install.packages("sparklyr", dependencies=TRUE)'
RUN R -e 'sparklyr::spark_install("2.4", verbose = TRUE)'
ADD "https://www.random.org/cgi-bin/randbyte?nbytes=10&format=h" skipcache
RUN wget https://github.com/HikaGenji/sparkavroudf/archive/master.zip && \
unzip master.zip
WORKDIR sparkavroudf-master
RUN R -e 'sparklyr::compile_package_jars(spec = sparklyr::spark_compilation_spec(spark_version = "2.4", jar_name = "sparklyudf-2.4-5_2.11.jar", jar_dep = list(normalizePath("deps/spark-avro_2.11-2.4.5.jar"), normalizePath("deps/avro-1.9.2.jar"), normalizePath("deps/kafka-avro-serializer-5.4.1.jar"), normalizePath("deps/kafka-schema-registry-client-5.4.1.jar"), normalizePath("deps/kafka-schema-registry-5.4.1.jar"), normalizePath("deps/kafka-clients-5.4.1-ce.jar"))))'
WORKDIR /
RUN R CMD build sparkavroudf-master
RUN R -e 'install.packages("sparklyudf_0.1.0.tar.gz", repos=NULL)'
RUN cp -r /root/spark/spark-2.4.5-bin-hadoop2.7 /home/rstudio/spark

ENV SPARK_HOME=/home/rstudio/sparkspark-2.4.5-bin-hadoop2.7/

ENV JAVA_HOME /usr/lib/jvm/java-8-oracle

RUN chown -R rstudio:rstudio /home/rstudio/spark