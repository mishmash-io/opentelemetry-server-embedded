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

FROM maven AS builder

WORKDIR /tmp/maven-deps
RUN --mount=type=bind,source=.,target=/tmp/maven-deps,rw,Z cd /tmp/maven-deps && \
    mvn -DskipTests clean install

FROM apache/druid:35.0.1

WORKDIR /opt/druid
RUN mkdir /opt/druid/.m2
RUN --mount=type=bind,from=builder,source=/root/.m2,target=/opt/druid/.m2 bin/run-java \
    -classpath "lib/*" org.apache.druid.cli.Main tools pull-deps \
    --no-default-hadoop -c "io.mishmash.opentelemetry:druid-otlp-format:1.1.3"
