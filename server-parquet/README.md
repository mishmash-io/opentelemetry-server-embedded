# Stand-alone OpenTelemetry Server

This simple stand-alone server launches the [embeddable collectors](../collector-embedded) for OpenTelemetry (logs, metrics, traces and profiles) and saves the received signals in Apache Parquet files.

It's intended to be used locally when running tests on the local
computer or as a simple way to collect telemetry and analyze it
when experimenting.

***WARNING:*** This server is completely open - it has no authentication nor authorization mechanisms in it! Use it only
locally when you are sure your networks are safe!

## Why saving OpenTelemetry to parquet files?

Once you've collected some files you can launch a local Apache Druid and Apache Superset to explore your logs, metrics, traces and profiles. Or you can launch a quick python notebook and use the
files in a Pandas DataFrame.

To find out more about how we use it (and get some extra tools) - see our [OpenTelemetry at mishmash io docs here.](https://mishmash.io/open_source/opentelemetry)

## Data Schema

The schema of the files is basically a flattened version of the
original OTEL protocol data - where instead of saving one row per
OTEL packet we flatten it to one row per record contained in a packet - one log entry, one metric data point or one trace span.

This implementation also extracts some record fields to individual
columns for simplicity when querying.

Details about each file schema can be found in its protobuf spec
 - [log file spec](src/main/proto/io/mishmash/opentelemetry/server/persistence/proto/v1/logs_persistence.proto),
 - [metric data point file spec](src/main/proto/io/mishmash/opentelemetry/server/persistence/proto/v1/metrics_persistence.proto) and
 - [span file spec](src/main/proto/io/mishmash/opentelemetry/server/persistence/proto/v1/traces_persistence.proto)
 - [profile file spec](src/main/proto/io/mishmash/opentelemetry/server/persistence/proto/v1/profiles_persistence.proto).

***Warning:*** The OpenTelemetry profiles signal is still considered **experimental!**

## Using the server

We believe the best use of this server is our [Testcontainers](https://testcontainers.com/) implementation that uses it to
collect telemetry from your unit and code tests. Visit [our docs page](https://mishmash.io/open_source/opentelemetry) to see how to do that.

But, if you've got something else in mind, here's how to use the
code contained here to launch the parquet server.

Using Podman or Docker to run in a container:

1. Pull from Docker Hub:
    ```bash
    docker pull mishmashio/opentelemetry-parquet-server
    ```
    ```bash
    podman pull mishmashio/opentelemetry-parquet-server
    ```
2. Launch ***(Note: Change /path/on/host/where/to/save/files to a directory on your computer where you want to save OpenTelemetry data)***
    ```bash
    docker run -p 4317:4317 -p 4318:4318 -v /path/on/host/where/to/save/files:/parquet:z mishmashio/opentelemetry-parquet-server
    ```
    ```bash
    podman run -p 4317:4317 -p 4318:4318 --mount=type=bind,src=/path/on/host/where/to/save/files,dst=/parquet,relabel=shared mishmashio/opentelemetry-parquet-server
    ```

Using Apache Maven:

1. Clone this repository
2. Run the following command to build the code:
    ```bash
    mvn compile
    ```
3. Launch:
    ```bash
    mvn exec:java -Dexec.mainClass="io.mishmash.opentelemetry.server.parquet.CollectorsMain" 
    ```

## Experimenting with OpenTelemetry

This server is also instrumented with OpenTelemetry and can produce its own logs, metrics and traces (but no profiles!). Meaning - for a very quick way to collect some telemetry data your can plug-in the
OpenTelemetry java agent and point it back to this server.

This will effectively save this server's telemetry in the output parquet files. It's not a very good idea as receiving its own telemetry will make the server produce even more telemetry, but it can be a very quick way to get some OpenTelemetry data.

To do that clone this repository and build the code (steps 1. and 2. above) and then:

1. Download the OpenTelemetry [Java auto-instrumentation agent](https://opentelemetry.io/docs/languages/java/automatic/#setup) and save it locally
2. Configure the agent by setting some environment variables:
    ```bash
    export OTEL_SERVICE_NAME="your-service-name"
    export OTEL_EXPORTER_OTLP_PROTOCOL="grpc"
    export OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4317"
    ```
3. Launch the server with the Java agent (note: you need to update the path to the agent jar):
    ```bash
    mvn exec:exec -Dexec.executable="java" -Dexec.args="-javaagent:/path-of-agent-jar/opentelemetry-javaagent.jar -classpath %classpath io.mishmash.opentelemetry.server.parquet.CollectorsMain"
    ```

Wait a bit until some records are collected into the parquet files and then stop the server with Control-C.

## Configuring the server

As a simple stand-alone OpenTelemetry server intended for small-scale local uses only there's not much in terms of configuration.

At the moment you can change where the parquet files will be saved by setting (before launching):

```bash
export PARQUET_LOCATION="/new/location"
```

If you don't set this environment variable files will be saved in the current working directory of the jvm.

Also, you can extend each parquet file with custom metadata by setting your own environment variables. Just prefix their names with `PARQUET_META_`, like this:

```bash
export PARQUET_META_my_variable="my_value"
```

The above will add `my_variable=my_value` to the list of parquet extra file metadata fields. You can set a number of such variables and later use the saved metadata to identify related parquet files.

## About the code

This server, and especially its [CollectorsMain class](src/main/java/io/mishmash/opentelemetry/server/parquet/CollectorsMain.java) is a simple example on how to implement and embed more complex OpenTelemetry data sources with the help of [our OpenTelemetry embeddable collectors.](../collector-embedded/)

### Compiling

Do not build this package directly unless absolutely necessary. Instead, build the [parent project.](../README.md)

### Building the Docker image

To build the Docker image:
1. Make sure the code here is compiled (see above).
2. Get the dependencies as they have to be copied to the container image:
    ```bash
    mvn dependecy:copy-dependencies
    ```
3. Build the image with podman or docker:
    ```bash
    docker build -f Dockerfile
    ```
    ```bash
    podman build -f Dockerfile
    ```
