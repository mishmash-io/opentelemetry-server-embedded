# Embeddable OpenTelemetry data collectors for Java

This package contains [mishmash io](https://mishmash.io) implementations of collectors for OpenTelemetry logs, metrics, traces and profiles that we use to develop data sources for other systems like [Apache Pulsar](https://pulsar.apache.org/), [Apache Druid](https://druid.apache.org/) and other distributed systems that have their own mechanisms to manage (like, for example - to scale up and down) their data ingestion.

Instead of using the [Collector provided by OpenTelemetry](https://opentelemetry.io/docs/collector/) sometimes we prefer to reduce the number of components in a large system by reusing (and extending) functionality that's already provided by another component.

To find out more about how we use it (and to get some extra tools) - see our [OpenTelemetry at mishmash io docs here.](https://mishmash.io/open_source/opentelemetry)

## How to use this code

The documentation of the code can be found in its javadoc.

Perhaps the simplest example of how to build your own OpenTelemetry data source is to [check out the stand-alone parquet server.](../server-parquet)

### Add it to your project

With Apache Maven:

- Add to your pom.xml:
```xml
<dependency>
  <groupId>io.mishmash.opentelemetry</groupId>
  <artifactId>collector-embedded</artifactId>
  <version>1.1.0</version>
</dependency>
```
