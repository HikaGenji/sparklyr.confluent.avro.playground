# sparklyr-avro
read avro encoded kafka topic into sparklyr

Steps to replicate:

- produce some avro data with avroProducer.py:
  docker run --rm -it --network host -v /home/vagrant:/app hikagenji/confluent-kafka-avro-python:latest python /app/avro-producer.py
  
inside rstudio container:
- read the data from spark-shell with readAvro.scala:
  /home/rstudio/spark/spark-3.0.0-preview-bin-hadoop3.2/bin/spark-shell --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0-preview,org.apache.spark:spark-avro_2.12:3.0.0-preview
  - this properly decodes avro struct:
    df: org.apache.spark.sql.DataFrame = [value: struct<timestamp: timestamp, side: int ... 1 more field>]
  
- read the data with R: readAvro.R
  - fails when using sql: 
    Error: org.apache.spark.sql.AnalysisException: Undefined function: 'from_avro'. This function is neither a registered temporary   function nor a permanent function registered in the database 'default'.;
  - fails when using invoke:
    Error: java.lang.ClassNotFoundException: org.apache.spark.sql.avro.functions
