#    Copyright 2024 Mishmash IO UK Ltd.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

FROM mishmashio/stacks-base:latest AS base

COPY target/server-parquet-*[0-9\.].jar /opt/stacks/otlp-server-parquet/lib/
COPY . /tmp/server-parquet

RUN cd /tmp/server-parquet && mvn --offline -Dbase.path="/opt/stacks" clean initialize

FROM eclipse-temurin:25-jre-noble@sha256:d9a39a23634650173f1e2bbc176227af9728587ecf0f4b62d53e9355cd7a19ab AS server-parquet

COPY --from=mishmashio/stacks-gson:latest /opt/stacks/ /opt/stacks/
COPY --from=mishmashio/stacks-protobuf:latest /opt/stacks/ /opt/stacks/
COPY --from=mishmashio/stacks-logging:latest /opt/stacks/ /opt/stacks/
COPY --from=mishmashio/stacks-slf4j:latest /opt/stacks/ /opt/stacks/
COPY --from=mishmashio/stacks-yaml:latest /opt/stacks/ /opt/stacks/
COPY --from=mishmashio/stacks-jackson:latest /opt/stacks/ /opt/stacks/
COPY --from=mishmashio/stacks-jackson-yaml:latest /opt/stacks/ /opt/stacks/
COPY --from=mishmashio/stacks-netty:latest /opt/stacks/ /opt/stacks/
COPY --from=mishmashio/stacks-vertx-server-grpc:latest /opt/stacks/ /opt/stacks/
COPY --from=mishmashio/stacks-vertx-server-web:latest /opt/stacks/ /opt/stacks/
COPY --from=mishmashio/stacks-opentelemetry-api:latest /opt/stacks/ /opt/stacks/

COPY --from=mishmashio/stacks-opentelemetry-agent:latest /opt/stacks/ /opt/stacks/
COPY --from=mishmashio/stacks-shellprofiles-container:latest /opt/stacks/ /opt/stacks/

COPY --from=base /opt/stacks/ /opt/stacks/

#ADD ./target/dependency /opt/server-parquet
#COPY target/server-parquet-*.jar /opt/server-parquet
#RUN mkdir /opt/opentelemetry
#RUN mv /opt/server-parquet/opentelemetry-javaagent-*.jar /opt/opentelemetry/opentelemetry-javaagent.jar
RUN chmod -R 755 "/opt/stacks/otlp-server-parquet/bin/otlp-server-parquet"
RUN mkdir -p /parquet

EXPOSE 4317 4318
ENV PARQUET_LOCATION=/parquet
ENV OWN_TELEMETRY_ENABLE=false
ENV OTEL_SERVICE_NAME="otlp-server-parquet"
ENV OTEL_EXPORTER_OTLP_PROTOCOL="grpc"
ENV OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4317"

#ENTRYPOINT [ "/bin/sh", "-c", "java `[ \"$(echo $OWN_TELEMETRY_ENABLE | tr '[:lower:]' '[:upper:]')\" == \"TRUE\" ] && echo \"-javaagent:/opt/opentelemetry/opentelemetry-javaagent.jar\"` -cp /opt/server-parquet/\\* io.mishmash.opentelemetry.server.parquet.CollectorsMain" ]
ENTRYPOINT ["/__cacert_entrypoint.sh", "/opt/stacks/otlp-server-parquet/bin/otlp-server-parquet"]
CMD ["run"]
