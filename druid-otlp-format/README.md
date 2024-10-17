# Apache Druid extension for OpenTelemetry singals ingestion

![druid-otlp-ingestion](https://github.com/user-attachments/assets/1b6d064e-7335-4365-a694-8cc2eebf1348)

This artifact implements an Apache Druid extension that you can use to ingest 
[OpenTelemetry](https://opentelemetry.io) signals - `logs`, `metrics`, `traces` and `profiles` - into Apache Druid, and then query Druid through interactive charts and dashboards.

## What is OpenTelemetry?

OpenTelemetry is high-quality, ubiquitous, and portable telemetry that enables effective observability.

It's a newer generation of telemetry systems that builds on the experience gained with earlier
implementations and offers a number of improvements. We, at [mishmash io,](https://mishmash.io) find
OpenTelemetry particularly useful - see
[the reasons here](../README.md#why-you-should-switch-to-opentelemetry), and
[how we use it in our development process here.](https://mishmash.io/open_source/opentelemetry)

Also, make sure you check [the OpenTelemetry official docs.](https://opentelemetry.io/docs/)

## Why Apache Druid?

Apache Druid is a high performance, real-time analytics database that delivers sub-second queries on streaming and batch data at scale and under load. It performs particularly well on `timestamped`
data, and OpenTelemetry signals are heavily timestamped. See [Apache Druid Use Cases](https://druid.apache.org/use-cases) for more.

# Quick introduction

To get an idea of why and when to use this Druid extension - here is an example setup:

1. Setup [Apache Kafka.](https://kafka.apache.org/)
2. Get the [OpenTelemetry collector](https://opentelemetry.io/docs/collector/) and configure it
   to export data to Kafka. Use the [OpenTelemetry Kafka Exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/kafkaexporter/README.md) provided by OpenTelemetry.
3. Setup Apache Druid with this artifact as an extension.
4. Inside Druid, configure [Kafka Streaming Ingestions](https://druid.apache.org/docs/latest/ingestion/streaming/) for `logs`, `metrics` and `traces`.

   In the ingestion specs set the `InputFormat` to this extension (more on this below).
   
   ***Note:*** OpenTelemetry `profiles` signal is still in development and is not supported
   by the Kafka Exporter.
5. Setup [Apache Superset](https://superset.apache.org/) with a [Druid database driver.](https://superset.apache.org/docs/configuration/databases#apache-druid)
6. Explore your telemetry in Superset!

![superset-dashboard](https://github.com/user-attachments/assets/8dba1e13-bcb3-41c9-ac40-0c023a3825c8)

> [!TIP]
> We have prepared a clone of [OpenTelemetry's demo app](https://opentelemetry.io/docs/demo/) with
> the exact same setup as above.
>
> It's quicker to run (no configuration needed) and will also generate telemetry to populate your
> Druid tables.
>
> Get our [Druid ingestion demo app fork here.](https://github.com/mishmash-io/opentelemetry-demos)

> [!NOTE]
> There are more ways of ingesting OpenTelemetry data into Apache Druid.
>
> Watch [this repository](https://github.com/mishmash-io/opentelemetry-server-embedded) for updates
> as we continue to publish our internal OpenTelemetry-related code and add more examples!

# Installation

In order to use this extension you need to install it inside your Druid nodes. This is done
through the typical [extension-loading process](https://druid.apache.org/docs/latest/configuration/extensions/):

1. Pull the extension:
  ```bash
  cd /opt/druid && \
  bin/run-java \
    -classpath "lib/*" org.apache.druid.cli.Main tools pull-deps \
    --no-default-hadoop -c "io.mishmash.opentelemetry:druid-otlp-format:1.1.3"
  ```
2. Enable it in the Druid configuration:
  ```
  druid.extensions.loadList=[<existing extensions>, "druid-otlp-format"]
  ```

That's all! Now launch your Druid instances and you're ready to ingest OpenTelemetry data!

## Using a Docker image

We've prepared an [Apache Druid for OpenTelemetry Docker image](https://hub.docker.com/repository/docker/mishmashio/druid-for-opentelemetry) that lets you skip the installation steps above and just
launch a Druid cluster with our OpenTelemetry extenion preinstalled.

It is based on the [official image](https://druid.apache.org/docs/latest/tutorials/docker/) and is
configured and launched the same way. Just replace the tags with `mishmashio/druid-for-opentelemetry` in your `docker-compose.yaml.`

Don't forget to enable the extension when launching!

## Building your own Docker image

If you're building your own, custom Druid image - just include the `pull-deps` installation step
above.

***Note:*** The `pull-deps` command needs to open a number of files simultaneously, and the maximum
number of open files inside a container might be limited on your system. On some Linux distributions
`podman`, for example, has limits that are too low.

If this turns out to be the case on your computer, raise the `number of open files` limit during the
build:

```sh
podman build --ulimit nofile=65535:65535 -f Dockerfile ..
```

# Configuring ingestion jobs

Once the `druid-otlp-format` extension is loaded, you can set Druid to start loading OpenTelemetry
data.

To create a data import task you need to provide Druid with an `ingestion spec`, detailing, among
other things - where it should look for data, what the data format is, etc.

To apply this extension to your data source - write an `ingestion spec` the way you normally would,
just set the ***InputFormat*** section to:

- when ingesting `logs` signals:
  - when produced by the **OpenTelemetry Kafka Exporter:**
    ```json
    {
      ...
      "spec": {
        "ioConfig": {
          ...
          "inputFormat": {
            "type": "otlp",
            "otlpInputSignal": "logsRaw"
          },
          ...
        },
      ...
      }
      ...
    }
    ```
  - when produced by other modules published by **mishmash io:**
    ```json
    {
      ...
      "spec": {
        "ioConfig": {
          ...
          "inputFormat": {
            "type": "otlp",
            "otlpInputSignal": "logsFlat"
          },
          ...
        },
      ...
      }
      ...
    }
    ```
- when ingesting `metrics` signals:
  - when produced by the **OpenTelemetry Kafka Exporter:**
    ```json
    {
      ...
      "spec": {
        "ioConfig": {
          ...
          "inputFormat": {
            "type": "otlp",
            "otlpInputSignal": "metricsRaw"
          },
          ...
        },
      ...
      }
      ...
    }
    ```
  - when produced by other modules published by **mishmash io:**
    ```json
    {
      ...
      "spec": {
        "ioConfig": {
          ...
          "inputFormat": {
            "type": "otlp",
            "otlpInputSignal": "metricsFlat"
          },
          ...
        },
      ...
      }
      ...
    }
    ```
- when ingesting `traces` signals:
  - when produced by the **OpenTelemetry Kafka Exporter:**
    ```json
    {
      ...
      "spec": {
        "ioConfig": {
          ...
          "inputFormat": {
            "type": "otlp",
            "otlpInputSignal": "tracesRaw"
          },
          ...
        },
      ...
      }
      ...
    }
    ```
  - when produced by other modules published by **mishmash io:**
    ```json
    {
      ...
      "spec": {
        "ioConfig": {
          ...
          "inputFormat": {
            "type": "otlp",
            "otlpInputSignal": "tracesFlat"
          },
          ...
        },
      ...
      }
      ...
    }
    ```
- when ingesting `profiles` signals:
  - when produced by the **OpenTelemetry Kafka Exporter:**
    
    > `Profiles` are not yet supported by the Kafka Exporter
  - when produced by other modules published by **mishmash io:**
    ```json
    {
      ...
      "spec": {
        "ioConfig": {
          ...
          "inputFormat": {
            "type": "otlp",
            "otlpInputSignal": "profilesFlat"
          },
          ...
        },
      ...
      }
      ...
    }
    ```

An example for a `logs` ingestion spec loading data produced by the OpenTelemetry Kafka Exporter might look like this:

```json
{
  "type": "kafka",
  "spec": {
    "ioConfig": {
      "type": "kafka",
      "consumerProperties": {
        "bootstrap.servers": "localhost:9092"
      },
      "topic": "otlp_logs",
      "inputFormat": {
        "type": "otlp",
        "otlpInputSignal": "logsRaw"
      },
      "useEarliestOffset": true
    },
    "tuningConfig": {
      "type": "kafka"
    },
    "dataSchema": {
        ...
      },
      "granularitySpec": {
        ...
      }
    }
  }
}
```

If you're not very familiar with `ingestion specs` - take a look at [Apache Druid Kafka tutorial](https://druid.apache.org/docs/latest/tutorials/tutorial-kafka) to get started.

## Using Druid GUI console

Druid's GUI console provides an interactive way for you to configure your ingestion jobs, without
actually writing JSON specs.

Unfortunately, at the moment we do not support configuring the OTLP Input Format extension through
the GUI console.

If you feel a bit brave - you can still use the GUI, with a little 'hack' - when you get to the
`Parse data` step on the GUI wizard - switch to the `Edit Spec` step where you'll see the JSON
that is being prepared. It has the same format as above and you can paste the correct `inputFormat`
parameters (take the correct config from above). Once you do that - switch back to the `Parse data`
tab and voila!

# OpenTelemetry at mishmash io

OpenTelemetry's main intent is the observability of production environments, but at [mishmash io](https://mishmash.io) it is part of our software development process. By saving telemetry from  **experiments** and **tests** of 
our own algorithms we ensure things like **performance** and **resource usage** of our distributed database, continuously and across releases.

We believe that adopting OpenTelemetry as a software development tool might be useful to you too, which is why we decided to open-source the tools we've built.

Learn more about the broader set of [OpenTelemetry-related activities](https://mishmash.io/open_source/opentelemetry) at
[mishmash io](https://mishmash.io/) and `follow` [GitHub profile](https://github.com/mishmash-io) for updates and new releases.
