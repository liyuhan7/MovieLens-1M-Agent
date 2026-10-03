# Hadoop 3.3.6 local-MapReduce runtime, built entirely from resources already present.
# The base image supplies OpenJDK 11; Hadoop's archive is verified/downloaded separately.
FROM wurstmeister/kafka:latest
COPY hadoop-3.3.6.tar.gz /tmp/hadoop.tar.gz
RUN mkdir -p /opt && tar -xzf /tmp/hadoop.tar.gz -C /opt && rm /tmp/hadoop.tar.gz
ENV JAVA_HOME=/usr/local/openjdk-11 \
    HADOOP_HOME=/opt/hadoop-3.3.6 \
    HADOOP_CONF_DIR=/opt/hadoop-3.3.6/etc/hadoop \
    PATH=/opt/hadoop-3.3.6/bin:/opt/hadoop-3.3.6/sbin:/usr/local/openjdk-11/bin:$PATH
# The default configuration uses file:/// and mapreduce.framework.name=local;
# actual Hadoop MapReduce jobs run inside this container, without a mock backend.
ENTRYPOINT ["/bin/sh", "-c"]
CMD ["hadoop version"]
