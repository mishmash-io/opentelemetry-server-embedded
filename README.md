# OpenTelemetry Data Sources for Java (and other)

This repository contains [OpenTelemetry](https://opentelemetry.io/) servers that can be embedded into other Java-based systems to act as data sources for `logs`, `metrics`, `traces` and `profiles` signals.

Here you can also find implementations of such data sources for a few popular open source softwares and additional tools to use when working with OpenTelemetry data.

You will also find additional tools, examples and demos that might be of service on your own OpenTelemetry journey.

> [!TIP]
> This is a public release of code we have accumulated internally over time and so far contains only a limited subset of what we intend to share.
>
> Examples of internal software that will be published here in the near future include:
> 
> - A small OTLP server based on [Apache BookKeeper](https://bookkeeper.apache.org/) for improved
>   data ingestion reliability, even across node failures
> - OpenTelemetry Data Sources for [Apache Pulsar](https://pulsar.apache.org/) for when more
>   more complex preprocessing is needed
> - Our [Testcontainers](https://testcontainers.com/) implementations that you can use to
>   ensure your apps always produce the necessary telemetry, or to track performance across
>   releases
>
> Watch this repository for updates.

***Contents:***

- [How OpenTelemetry compares to other telemetry software](#why-you-should-switch-to-opentelemetry)
- [Introduction to OpenTelemetry for Developers, Data Engineers and Data Scientists](#opentelemetry-for-developers-data-engineers-and-data-scientists)
- [Software artifacts to:](#artifacts)
  - [Embed OTLP collectors in Java systems](#embeddable-collectors)
  - [Save OpenTelemetry to Apache Parquet files](#apache-parquet-stand-alone-server)
  - [Ingest OpenTelemetry into Apache Druid](#apache-druid-otlp-input-format)
  - [Visualize OpenTelemetry with Apache Superset](#apache-superset-charts-and-dashboards)
- [More about OpenTelemetry at mishmash io](#opentelemetry-at-mishmash-io)

# Why you should switch to OpenTelemetry

If you are new to OpenTelemetry you might be thinking how is it better than the multitude of
existing telemetry implementations, many of which are already available or well established within
popular runtimes like Kubernetes, for example.

There are a number of advantages that OpenTelemetry offers compared to earlier telemetries:

- All signal types (`logs`, `metrics`, `traces` and `profiles`) are ***correlatable:***
  
  For exmpale - you can explore ***only*** the `logs` emitted inside a given (potentially failing) `span`.

  To see how `telemetry signal correlation` works - refer to the [OpenTelemetry for Developers, Data Engineers and Data Scientists](#opentelemetry-for-developers-data-engineers-and-data-scientists) examples section below.
- More precise timing:
  
  Unlike other telemetries, OpenTelemetry does not `pull` data, it `pushes` it. By avoiding the
  extra request needed to pull data - OpenTelemetry reports much more accurate timestamps of
  when `logs` or `spans` and other events where emitted, or `metrics` values were updated.
- Zero-code telemetry:
  
  You can add telemetry to your existing apps without any code modifications. If you're using
  popular frameworks - they already have OpenTelemetry instrumentation that will just work out
  of the box. See the [OpenTelemetry docs for your programming language.](https://opentelemetry.io/docs/languages/)

  Also, you do not need to implement special endpoints and request handlers to supply telemetry.
- No CPU overhead if telemetry is not emitted:
  
  When code instrumented with OpenTelemetry runs ***without*** a configured signals exporter
  (basically when it is disabled) - all OpenTelemetry API methods are basically empty.

  They do not perform any operations, thus not requiring any CPU. 
- Major companies already support OpenTelemetry:
  
  Large infrastructure providers - public clouds like Azure, AWS and GCP already seamlessly integrate their monitoring and observability services with OpenTelemetry.
  
  Instrumenting your code with OpenTelemetry means it can be monitored on any of them, without
  code changes.

If the above sounds convincing - keep reading through this document and explore the links in it.

# OpenTelemetry for Developers, Data Engineers and Data Scientists

We have prepared a few Jupyter notebooks that visually explore OpenTelemetry data that we collected from [a demo Astronomy webshop app](https://github.com/mishmash-io/opentelemetry-demos)
using the [Apache Parquet Stand-alone server](./server-parquet) contained in this repository.

If you are the sort of person who prefers to learn by looking at **actual data** - start with the [OpenTelemetry Basics Notebook.](./examples/notebooks/basics.ipynb)

> [!TIP]
> If you're wondering how to get your first OpenTelemetry data sets - check out [our fork of OpenTelemetry's Demo app.](https://github.com/mishmash-io/opentelemetry-demos)
>
> In there you will find complete deployments that will generate signals, save them and let you play with the data - by writing your own notebooks or creating
> Apache Superset dashboards.
> 

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
- [Quick deployment with a demo app](https://github.com/mishmash-io/opentelemetry-demos)

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
- [Quick deployment with a demo app and Apache Superset](https://github.com/mishmash-io/opentelemetry-demos)

## Apache Superset charts and dashboards

![superset-dashboard](https://github.com/user-attachments/assets/8dba1e13-bcb3-41c9-ac40-0c023a3825c8)

[Apache Superset](https://superset.apache.org/) is an open-source modern data exploration and visualization platform.

You can use its rich visualizations, no-code viz builder and its powerful SQL IDE to build your own OpenTelemetry analytics.

To get you started, we're publishing [data sources and visualizations](./superset-visualizations) that you can import into Apache Superset.

- [Quick deployment with a demo app](https://github.com/mishmash-io/opentelemetry-demos)
  
# OpenTelemetry at mishmash io

OpenTelemetry's main intent is the observability of production environments, but at [mishmash io](https://mishmash.io) it is part of our software development process. By saving telemetry from  **experiments** and **tests** of 
our own algorithms we ensure things like **performance** and **resource usage** of our distributed database, continuously and across releases.

We believe that adopting OpenTelemetry as a software development tool might be useful to you too, which is why we decided to open-source the tools we've built.

Learn more about the broader set of [OpenTelemetry-related activities](https://mishmash.io/open_source/opentelemetry) at
[mishmash io](https://mishmash.io/) and `follow` [GitHub profile](https://github.com/mishmash-io) for updates and new releases.
