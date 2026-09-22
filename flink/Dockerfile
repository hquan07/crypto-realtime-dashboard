FROM flink:1.19-scala_2.12

# Install Python3 and pip
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev && \
    rm -rf /var/lib/apt/lists/*

# Install PyFlink and download jars
RUN ln -s /usr/bin/python3 /usr/bin/python
RUN pip3 install apache-flink==1.19.0

# Download Connectors
RUN wget -P /opt/flink/lib https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar
RUN wget -P /opt/flink/lib https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar
RUN wget -P /opt/flink/lib https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.0-patch5/clickhouse-jdbc-0.6.0-patch5-all.jar
