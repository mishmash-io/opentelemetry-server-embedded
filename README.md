# OpenTelemetry and Apache Big Data, United by mishmash io

This repository contains code that receives and adapts [OpenTelemetry](https://opentelemetry.io/) signals - like `logs`, `metrics`, `traces` and `profiles` - to Open Source projects of the [Apache](https://www.apache.org/) analytics ecosystem. Whether you're building complex observability pipelines or just getting started, you'll find useful resources here.

**Create powerful Observability analytics backends** by blending and bundling these signals for:

- **Batch processing** with [Apache Spark](https://spark.apache.org/) or [Apache Hive](https://hive.apache.org/)
- **Real-time analytics** with [Apache Druid](https://druid.apache.org/) and [Apache Superset](https://superset.apache.org/)
- **Machine Learning and AI** workflows

> [!TIP]
> This public release includes code we have accumulated internally over time, and we are actively developing additional tools and examples to help you maximize the potential of [OpenTelemetry](https://opentelemetry.io/) and [Apache](https://www.apache.org/) Big Data projects.
>
> Examples of internal software that will be published here in the near future includes:
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

- [Why you should switch to OpenTelemetry](#why-you-should-switch-to-opentelemetry)
- [OpenTelemetry for Developers, Data Engineers and Data Scientists](#opentelemetry-for-developers-data-engineers-and-data-scientists)
- [When and where should you use the software in this repository](#when-and-where-should-you-use-the-software-in-this-repository)
- [Software artifacts to:](#artifacts)
  - [Embed OTLP collectors in Java systems](#embeddable-collectors)
  - [Save OpenTelemetry to Apache Parquet files](#apache-parquet-stand-alone-server)
  - [Ingest OpenTelemetry into Apache Druid](#apache-druid-otlp-input-format)
  - [Visualize OpenTelemetry with Apache Superset](#apache-superset-charts-and-dashboards)
- [OpenTelemetry at mishmash io](#opentelemetry-at-mishmash-io)

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

> [!TIP]
> If you are the sort of person who prefers to learn by looking at **actual data** - start with the [OpenTelemetry Basics Notebook.](./examples/notebooks/basics.ipynb)

# When and where should you use the software in this repository

We, at [mishmsah io,](https://mishmash.io/) have been using OpenTelemetry for quite some time - recording telemetry from experiments, unit and integration tests - to ensure every new release
of software we develop is performing better than the last, and within reasonable computing-resource usage. (More on this [here.](https://mishmash.io/open_source/opentelemetry))

> [!TIP]
> OpenTelemetry is great for **monitoring software in production,** but we believe you should adopt it within your **software development process** too.

Having been through that journey ourselves, we've realised that success depends on strong analytics. OpenTelemetry provides a number of tools to [instrument your code](https://opentelemetry.io/docs/concepts/instrumentation/) to emit signals, and then to compose data transmission pipelines for these signals. And leaves it to you to decide what you ultimately want to do with your signals: where you want to store them depends on how you will work with them.

You can compose such pipelines for signals transmition using the [OpenTelemetry Collector,](https://opentelemetry.io/docs/collector/) which in turn uses a network protocol called [OTLP.](https://opentelemetry.io/docs/specs/otel/protocol/) At the end - you have to `terminate` the pipelines into an `observability (or OTLP) backend.`

As a network protocol, OTLP is great at reducing the number of bytes transmitted, keeping the throughput high with minimum overhead. It does this by heavily `nesting` its messages - to avoid
data duplication and take maximum advantage of `dictionary encodings` and data compression.

On the **analytics side** though - heavily nested structures are not optimal. A simple `count(*)` or
`sum()` query, done over millions of OTLP messages, will have to `unnest` each one of them. Every time you run that query.

And this is the second reason why we believe you might find the software here useful:

> [!TIP]
> When doing analytics on your observability data - you need a suitable data schema.
>
> The tools in this repository convert OTLP messages into a 'flatter' schema, that's more suitable
> for analytics.
>
> They preform transformations, **only once** - on **OTLP packet reception,** to minimize the overhead that would otherwise be incurred every time you run an analytics job or query.

Following are quick introductions of the individual software packages, where you can find more information.

> [!TIP]
> If you're wondering how to get your first OpenTelemetry data sets - check out [our fork of OpenTelemetry's Demo app.](https://github.com/mishmash-io/opentelemetry-demos)
>
> In there you will find complete deployments that will generate signals, save them and let you play with the data - by writing your own notebooks or creating
> Apache Superset dashboards.
> 

# Software artifacts

## Embed OTLP collectors in Java systems

The base artifact - `collector-embedded` contains classes that handle the OTLP protocol (over both gRPC and HTTP).
- [README](./collector-embedded)
- [Javadoc on javadoc.io](https://javadoc.io/doc/io.mishmash.opentelemetry/collector-embedded)

## Save OpenTelemetry to Apache Parquet files

This artifact contains a runnable OTLP-protocol server that receives signals from OpenTelemetry and saves them into [Apache Paruqet](https://parquet.apache.org/) files.

It is not intended for production use, but rather as a quick tool to save and explore OpenTelemetry data locally. [The Basics Jupyter Notebook](./examples/notebooks/basics.ipynb) explores
Parquet files as saved by this Stand-alone server.
- [README](./server-parquet)
- [Javadoc on javadoc.io](https://javadoc.io/doc/io.mishmash.opentelemetry/server-parquet)
- [Quick deployment with a demo app](https://github.com/mishmash-io/opentelemetry-demos)

## Ingest OpenTelemetry into Apache Druid

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

## Visualize OpenTelemetry with Apache Superset

![superset-dashboard](https://github.com/user-attachments/assets/8dba1e13-bcb3-41c9-ac40-0c023a3825c8)

[Apache Superset](https://superset.apache.org/) is an open-source modern data exploration and visualization platform.

You can use its rich visualizations, no-code visualization builder and its powerful SQL IDE to build your own OpenTelemetry analytics dashboards.

To get you started, we are publishing [data sources and visualizations](./superset-visualizations) that you can import into Apache Superset.

- [Quick deployment with a demo app](https://github.com/mishmash-io/opentelemetry-demos)
  
# OpenTelemetry at mishmash io

[OpenTelemetry](https://opentelemetry.io/) is commonly used to monitor production environments, but at [mishmash io](https://mishmash.io), we also use it as a core part of our software development process. By capturing telemetry from **experiments** and **tests** of our own algorithms, we continuously ensure optimal **performance** and **resource usage** for our distributed database, across all releases.

We believe that OpenTelemetry can be a powerful tool not only for production monitoring but also for enhancing the development lifecycle. This belief is why we're open-sourcing the tools we've built—to help others benefit from using telemetry data to improve software quality and performance.

Learn more about our broader set of [OpenTelemetry-related activities](https://mishmash.io/open_source/opentelemetry) at [mishmash io](https://mishmash.io/) and make sure to `follow` our [GitHub profile](https://github.com/mishmash-io) for updates and new releases.
