# Apache Druid Ingestion Specs for OpenTelemetry

Here you can find example Apache Druid Ingestion Specs that you can use in combination with the
[Druid OTLP Extension](../../druid-otlp-format/) to setup ingestion jobs quickly.

Assuming the OTLP Extension is enabled inside Druid, you can just `HTTP POST` one or more of the
`JSON` files contained here:

For example, to setup an ingestion job for `logs`, as produced by the
[OpenTelemetry Kafka Exporter:](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/kafkaexporter/README.md)

> [!NOTICE]
> Replace `DRUID_ROUTER_HOST` and `DRUID_ROUTER_PORT` with the actual host name and port of
> your Druid Router instance.
>

```sh
curl -i -X POST http://{DRUID_ROUTER_HOST}:{DRUID_ROUTER_PORT}/druid/indexer/v1/supervisor --header 'Content-Type: application/json' --data-binary '@kafkaExporterLogsIngestionSpec.json'
```

For `metrics` ingestion use:

```sh
curl -i -X POST http://{DRUID_ROUTER_HOST}:{DRUID_ROUTER_PORT}/druid/indexer/v1/supervisor --header 'Content-Type: application/json' --data-binary '@kafkaExporterMetricsIngestionSpec.json'
```

And for `traces`:

```sh
curl -i -X POST http://{DRUID_ROUTER_HOST}:{DRUID_ROUTER_PORT}/druid/indexer/v1/supervisor --header 'Content-Type: application/json' --data-binary '@kafkaExporterTracesIngestionSpec.json'
```

> [!TIP]
>
> There are other ways of getting OpenTelemetry data into Apache Druid.
>
> [Learn more.](../../README.md)

Make sure you get a positive response from Druid. It should look similar to this:

```sh
HTTP/1.1 200 OK
Date: Thu, 17 Oct 2024 08:52:13 GMT
Date: Thu, 17 Oct 2024 08:52:13 GMT
Content-Type: application/json
Vary: Accept-Encoding, User-Agent
Content-Length: 18

{"id":"otlp_logs"}
```

Now, go to Druid GUI Console and you should be able to find your ingestion jobs there.
