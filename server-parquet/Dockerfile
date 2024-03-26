FROM eclipse-temurin:17-jre-alpine
ADD ./target/dependency /opt/server-parquet
COPY target/server-parquet-*.jar /opt/server-parquet
RUN mkdir -p /parquet

EXPOSE 4317 4318
ENV PARQUET_LOCATION=/parquet

ENTRYPOINT [ "java", "-cp", "/opt/server-parquet/*", "io.mishmash.opentelemetry.server.parquet.CollectorsMain"]
