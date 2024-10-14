# OpenTelemetry Data Sources for Java

This repository contains [OpenTelemetry](https://opentelemetry.io/) servers that can be embedded into other Java-based systems to act as data sources for `logs`, `metrics`, `traces` and `profiles` signals.

Here you can also find implementations of such data sources for a few popular open source softwares and additional tools to use when working with OpenTelemetry data.

> [!TIP]
> This is a public release of code we have accumulated internally over time and so far contains only a limited subset of what we intend to share.
>
> Examples of internal software that will be published here in the near future include:
> 
> - A small OTLP server based on [Apache BookKeeper](https://bookkeeper.apache.org/) for improved
>   data ingestion reliability, even across node failures
> - [Apache Superset](https://superset.apache.org/) charts and dashboards for OpenTelemetry
>   visualizations
> - OpenTelemetry Data Sources for [Apache Pulsar](https://pulsar.apache.org/) for when more
>   more complex preprocessing is needed
> - Our [Testcontainers](https://testcontainers.com/) implementations that you can use to
>   ensure your apps always produce the necessary telemetry, or to track performance across
>   releases
>
> Watch this repository for updates.

***Contents:***

- [Introduction to OpenTelemetry for Developers, Data Engineers and Data Scientists](#opentelemetry-for-developers-data-engineers-and-data-scientists)
- [Software artifacts to:](#artifacts)
  - [Embed OTLP collectors in Java systems](#embeddable-collectors)
  - [Save OpenTelemetry to Apache Parquet files](#apache-parquet-stand-alone-server)
  - [Ingest OpenTelemetry into Apache Druid](#apache-druid-otlp-input-format)
- [More about OpenTelemetry at mishmash io](#opentelemetry-at-mishmash-io)

# OpenTelemetry for Developers, Data Engineers and Data Scientists

We have prepared a few Jupyter notebooks that visually explore OpenTelemetry data that we collected from [a demo Astronomy webshop app](https://github.com/mishmash-io/opentelemetry-demo-to-parquet)
using the [Apache Parquet Stand-alone server](./server-parquet) contained in this repository.

If you are the sort of person who prefers to learn by looking at **actual data** - start with the [OpenTelemetry Basics Notebook.](./examples/notebooks/basics.ipynb)

# Artifacts

## Embeddable collectors

The base artifact - `collector-embedded` contains classes that handle the OTLP protocol (over both gRPC and HTTP).
- [README](./collector-embedded)
- [Javadoc on javadoc.io](https://javadoc.io/doc/io.mishmash.opentelemetry/collector-embedded)

## Apache Parquet Stand-alone server

This artifact contains a runnable OTLP-protocol server that receives signals from OpenTelemetry and saves them into [Apache Paruqet](https://parquet.apache.org/) files.

It is not intended for production use, but rather as a quick tool to save and explore OpenTelemetry data locally. [The Basics Jupyter Notebook](./examples/notebooks/basics.ipynb) explores
Parquet files as saved by this Stand-alone server.
- [README](./server-parquet)
- [Javadoc on javadoc.io](https://javadoc.io/doc/io.mishmash.opentelemetry/server-parquet)

## Apache Druid OTLP Input Format

Use this artifact when ingesting OpenTelemetry signals into [Apache Druid](https://druid.apache.org), in combination with an Input Source (like Apache Kafka or other).

Apache Druid is a high performance, real-time analytics database that delivers sub-second queries on streaming and batch data at scale and under load. This makes it perfect for OpenTelemetry data analytics.

With this OTLP Input Format you can build OpenTelemetry ingestion pipelines into Apache
Druid. For example:
- Use the [OpenTelemetry Kafka Exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/kafkaexporter/README.md) to publish
OTLP signals to an Apache Kafka topic, then the [Druid Kafka Ingestion](https://druid.apache.org/docs/latest/ingestion/kafka-ingestion/) with this Input Format to get Druid
tables with your telemetry.
- In a similar way you can also use other Druid input sources developed by mishmash io -
like with [Apache BookKeeper](https://bookkeeper.apache.org) or [Apache Pulsar](https://pulsar.apache.org). For details - check the related artifact documentation.

Find out more about the OTLP Input Format for Apache Druid:
- [README](./druid-otlp-format)
- [Javadoc on javadoc.io](https://javadoc.io/doc/io.mishmash.opentelemetry/druid-otlp-format)

# OpenTelemetry at mishmash io

OpenTelemetry's main intent is the observability of production environments, but at [mishmash io](https://mishmash.io) it is part of our software development process. By saving telemetry from  **experiments** and **tests** of 
our own algorithms we ensure things like **performance** and **resource usage** of our distributed database, continuously and across releases.

We believe that adopting OpenTelemetry as a software development tool might be useful to you too, which is why we decided to open-source the tools we've built.

Learn more about the broader set of [OpenTelemetry-related activities](https://mishmash.io/open_source/opentelemetry) at
[mishmash io](https://mishmash.io/) and `follow` [GitHub profile](https://github.com/mishmash-io) for updates and new releases.
