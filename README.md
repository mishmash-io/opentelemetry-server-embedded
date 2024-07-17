# OpenTelemetry Data Sources for Java

This repository contains OpenTelemetry servers that can be embedded into other Java-based systems to act as data sources
for logs, metrics, traces and profiles signals.

Here you can also find implementations of such data sources for a few popular open source softwares and additional tools
to use when working with OpenTelemetry data.

It is also part of a broader set of [OpenTelemetry-related activities](https://mishmash.io/open_source/opentelemetry) at
[mishmash io](https://mishmash.io/).

This is a public release of code we have accumulated internally over time and so far contains only a limited subset of
what we intend to share. Future releases will add modules for authentication and authorization, visualization and more.

Watch this repository for updates.

Also take a look at the READMEs of the individual packages in this repository:

- [embeddable OpenTelemetry collectors](./collector-embedded)
- [Apache Parquet Stand-alone server](./server-parquet)
