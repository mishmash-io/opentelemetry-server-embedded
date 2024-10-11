FROM maven AS builder

WORKDIR /tmp/maven-deps
RUN --mount=type=bind,source=.,target=/tmp/maven-deps,rw,Z cd /tmp/maven-deps && \
    mvn -DskipTests clean install

FROM apache/druid:30.0.1

WORKDIR /opt/druid
RUN mkdir /opt/druid/.m2
RUN --mount=type=bind,from=builder,source=/root/.m2,target=/opt/druid/.m2 bin/run-java \
    -classpath "lib/*" org.apache.druid.cli.Main tools pull-deps \
    --no-default-hadoop -c "io.mishmash.opentelemetry:druid-otlp-format:1.1.3"
