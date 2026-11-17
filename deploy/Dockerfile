FROM eclipse-temurin:11-jdk-jammy AS build
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends unzip && rm -rf /var/lib/apt/lists/*
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
RUN chmod +x mvnw && ./mvnw -B -q dependency:go-offline
COPY src/ src/
RUN ./mvnw -B -q -DskipTests package

FROM eclipse-temurin:11-jdk-jammy
ARG HADOOP_VERSION=3.4.2
RUN apt-get update && apt-get install -y --no-install-recommends curl procps ca-certificates && rm -rf /var/lib/apt/lists/*
RUN curl --fail --location --retry 3 "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" -o /tmp/hadoop.tar.gz \
    && curl --fail --location --retry 3 "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz.sha512" -o /tmp/hadoop.sha512 \
    && checksum=$(grep -Eo '[a-fA-F0-9]{128}' /tmp/hadoop.sha512 | head -n 1) \
    && test -n "$checksum" \
    && printf '%s  /tmp/hadoop.tar.gz\n' "$checksum" | sha512sum --check --status \
    && tar -xzf /tmp/hadoop.tar.gz -C /opt \
    && mv "/opt/hadoop-${HADOOP_VERSION}" /opt/hadoop \
    && rm /tmp/hadoop.tar.gz /tmp/hadoop.sha512
ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    HADOOP_COMMON_HOME=/opt/hadoop \
    HADOOP_HDFS_HOME=/opt/hadoop \
    HADOOP_YARN_HOME=/opt/hadoop \
    HADOOP_MAPRED_HOME=/opt/hadoop \
    HADOOP_LOG_DIR=/var/log/hadoop \
    HADOOP_HEAPSIZE_MAX=256 \
    HADOOP_ROOT_LOGGER=WARN,console \
    PATH=/opt/hadoop/bin:/opt/hadoop/sbin:$PATH
RUN useradd --create-home --uid 10001 hadoop \
    && mkdir -p /data/name /data/data /data/local /data/logs /var/log/hadoop /app/evidence /opt/demo \
    && chown -R hadoop:hadoop /data /var/log/hadoop /app /opt/demo /opt/hadoop
COPY --from=build --chown=hadoop:hadoop /build/target/hadoop-corpus-pipeline-2.0.0.jar /app/corpus.jar
COPY --chown=hadoop:hadoop deploy/config/ /opt/hadoop/etc/hadoop/
COPY --chown=hadoop:hadoop deploy/entrypoint.sh deploy/cluster-demo.sh /opt/demo/
RUN chmod +x /opt/demo/*.sh
USER hadoop
WORKDIR /app
ENTRYPOINT ["/opt/demo/entrypoint.sh"]
