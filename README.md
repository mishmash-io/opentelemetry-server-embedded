# OpenTelemetry Data Sources for Java

This repository contains [OpenTelemetry](https://opentelemetry.io/) servers that can be embedded into other Java-based systems to act as data sources
for logs, metrics, traces and profiles signals.

Here you can also find implementations of such data sources for a few popular open source softwares and additional tools
to use when working with OpenTelemetry data.

This is a public release of code we have accumulated internally over time and so far contains only a limited subset of
what we intend to share. Future releases will add modules for authentication and authorization, visualization and more.

Watch this repository for updates.

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

# OpenTelemetry at mishmash io

OpenTelemetry's main intent is the observability of production environments, but at [mishmash io](https://mishmash.io) it is part of our software development process. By saving telemetry from  **experiments** and **tests** of 
our own algorithms we ensure things like **performance** and **resource usage** of our distributed database, continuously and across releases.

We believe that adopting OpenTelemetry as a software development tool might be useful to you too, which is why we decided to open-source the tools we've built.

Learn more about the broader set of [OpenTelemetry-related activities](https://mishmash.io/open_source/opentelemetry) at
[mishmash io](https://mishmash.io/) and `follow` [GitHub profile](https://github.com/mishmash-io) for updates and new releases.

