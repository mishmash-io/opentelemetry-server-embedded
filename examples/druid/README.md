
```sh
curl -i -X POST http://{DRUID_ROUTER_HOST}:{DRUID_ROUTER_PORT}/druid/indexer/v1/supervisor --header 'Content-Type: application/json' --data-binary '@kafkaExporterLogsIngestionSpec.json'
```

```sh
curl -i -X POST http://{DRUID_ROUTER_HOST}:{DRUID_ROUTER_PORT}/druid/indexer/v1/supervisor --header 'Content-Type: application/json' --data-binary '@kafkaExporterMetricsIngestionSpec.json'
```

```sh
curl -i -X POST http://{DRUID_ROUTER_HOST}:{DRUID_ROUTER_PORT}/druid/indexer/v1/supervisor --header 'Content-Type: application/json' --data-binary '@kafkaExporterTracesIngestionSpec.json'
```

```sh
HTTP/1.1 200 OK
Date: Thu, 17 Oct 2024 08:52:13 GMT
Date: Thu, 17 Oct 2024 08:52:13 GMT
Content-Type: application/json
Vary: Accept-Encoding, User-Agent
Content-Length: 18

{"id":"otlp_logs"}
```

