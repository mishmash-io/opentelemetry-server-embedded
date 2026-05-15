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

FROM eclipse-temurin:21-jre-alpine
ADD ./target/dependency /opt/server-parquet
COPY target/server-parquet-*.jar /opt/server-parquet
RUN mkdir /opt/opentelemetry
RUN mv /opt/server-parquet/opentelemetry-javaagent-*.jar /opt/opentelemetry/opentelemetry-javaagent.jar
RUN mkdir -p /parquet

EXPOSE 4317 4318
ENV PARQUET_LOCATION=/parquet
ENV OWN_TELEMETRY_ENABLE=false
ENV OTEL_SERVICE_NAME="otlp-server-parquet"
ENV OTEL_EXPORTER_OTLP_PROTOCOL="grpc"
ENV OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4317"

ENTRYPOINT [ "/bin/sh", "-c", "java `[ \"$(echo $OWN_TELEMETRY_ENABLE | tr '[:lower:]' '[:upper:]')\" == \"TRUE\" ] && echo \"-javaagent:/opt/opentelemetry/opentelemetry-javaagent.jar\"` -cp /opt/server-parquet/\\* io.mishmash.opentelemetry.server.parquet.CollectorsMain" ]
