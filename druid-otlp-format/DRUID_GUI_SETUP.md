# Using the Apache Druid GUI to setup OpenTelemetry ingestion

This is a short, visual guide on how to use the [Apache Druid] GUI console to setup OpenTelemetry ingestion jobs using the
[druid-otlp-format extension](./README.md), developed by [mishmash io](https://mishmash.io).

To find more about the open source software contained in this repository - [click here.](../)

# Intro

At the time of writing the Druid GUI console does not directly allow configuring OTLP ingestion jobs. However, with a little 'hack',
it is possible.

Follow the steps below.

> [!WARNING]
> The guide here assumes you will be ingesting OpenTelemetry data published to Apache Kafka by the [Kafka Exporter.](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/kafkaexporter/README.md)
>
> In other setups the steps might be different.
> 

# Walk-through

#### 1. Open the console

Open the Druid GUI, select `Load data` from the top menu and then click `Streaming` in the dropdown:

![druid-step-1](https://github.com/user-attachments/assets/e0e6ed38-18da-4d99-b4f9-5c22f2510386)

#### 2. Start a new streaming spec

Click on the `Start a new streaming spec`:

![druid-step-2](https://github.com/user-attachments/assets/38b54817-d126-4bab-92c6-5dbae6c169e7)

#### 3. Select the source

When you start a new streaming spec you're given a choice to select where the data should come from. Click `Apache Kafka`:

![druid-step-3](https://github.com/user-attachments/assets/c9c16edc-b1b0-4478-be76-78b9377c1623)

#### 4. Connect to Kafka

To connect Druid to Kafka - enter your Kafka brokers and topic name in the right panel:

![druid-step-4](https://github.com/user-attachments/assets/06d63477-e188-44b2-be39-699c8e4bede0)

Click `Apply` when done. This will trigger Druid to connect and in a little while (if the connection was successful), Druid will
show some examples of what it read from the configured Kafka topic:

![druid-step-5](https://github.com/user-attachments/assets/c3280ab8-6372-46a3-aa81-f8b6c0b6a845)

At this step - the data will be messy - Druid doesn't yet know how to interpret it. It's okay, click the `Next: Parse data`
button in the right panel.

#### 5. Apply the OTLP format

Now, you're given a choice of what `Input format` to apply on the incoming data:

![druid-step-6](https://github.com/user-attachments/assets/c870c29a-2199-4b03-801b-ffd38dbea8d4)

Here's the tricky part - the `otlp` format is not available inside the `Input format` dropdown. You'll have to edit a bit of JSON
in order to get past this step.

On the bar just under the top menu, click on the final step - `Edit spec`. A JSON representation of the ingestion spec will be
shown to you. Edit the `inputFormat` JSON object:

![druid-step-7](https://github.com/user-attachments/assets/3429a010-5fa3-407d-ae3e-02f0eff1b8a7)

Use one of the following JSONs:
- when ingesting `logs`:
  ```json
  ...
  "inputFormat": {
    "type": "otlp",
    "otlpInputSignal": "logsRaw"
  }
  ...
  ```
- when ingesting `metrics`:
  ```json
  ...
  "inputFormat": {
    "type": "otlp",
    "otlpInputSignal": "metricsRaw"
  }
  ...
  ```
- when ingesting `traces`:
  ```json
  ...
  "inputFormat": {
    "type": "otlp",
    "otlpInputSignal": "tracesRaw"
  }
  ...
  ```

When done editing, just get back to the `Parse data` step, you don't need to click anywhere to confirm the changes you made.

Now, Druid will re-read the sampled Kafka messages and will parse them:

![druid-step-8](https://github.com/user-attachments/assets/d015b715-da30-4e15-bd06-861e1879350d)

At this point - click the `Next: Parse time` button in the right panel to continue.

#### 6. Configure the data schema

Now, Druid needs to know a few more things about how you'd like your data to be organized.

Select which column will be used for its **time-partitioning.** For example, click on the `time_unix_nano` column header,
make sure it shows in the right bar, and that `Format` says `nano`. Then click 'Apply`:

![druid-step-9](https://github.com/user-attachments/assets/016d6c6d-69d9-4fce-b63c-3b9319bb8103)

When done, click `Next: Transform`:

![druid-step-10](https://github.com/user-attachments/assets/6c166d4a-65b8-41cb-b69d-492a17a665a8)

Here you can configure optional transformations of the data before saving it inside Druid. Continue to the next step - `Filter`.

![druid-step-11](https://github.com/user-attachments/assets/879df0e6-834a-4cb2-b95a-61d94a5b1947)

Filtering is also an optional step. Continue to the `Configure schema` step.

Here you can edit the table schema. In the bar on the right - turn off the `Explicitly specify schema` radio button:

![druid-step-12pre](https://github.com/user-attachments/assets/7c10f38d-4ae9-4e46-82bd-51dd25ac5645)

Doing this will trigger a dialog to pop up, just confirm by clicking `Yes - auto detect schema`:

![druid-step-12](https://github.com/user-attachments/assets/828a8c9a-ff25-4821-bb7c-fe789eb04000)

Then proceed to the `Partition` step. Select a `Primary partitioning (by time)`. In this example, we're setting it to `hour`
for hourly partitions:

![druid-step-13](https://github.com/user-attachments/assets/6c6d1492-63fb-4188-baf4-b9b00eec53ce)

Continue to `Tune`:

![druid-step-14](https://github.com/user-attachments/assets/a67669ec-b7c4-4d02-bba5-3a1039de48ff)

Set the `Use earliest offset` to `True` and continue to `Publish`:

![druid-step-15](https://github.com/user-attachments/assets/1d49b0c0-02e0-487a-8bef-1bc75e0b126f)

See if you would like to rename the `Datasource name` or any of the other options, but the defaults should be okay. Move to the
next step - `Edit spec`:

![druid-step-16](https://github.com/user-attachments/assets/e8628c83-ec73-4cc7-9b90-c62d76ab8fba)

This is the same step we went to earlier in order to set the `InputFormat` to `otlp`. This time - there's nothing to do here,
so, just click `Submit` in the right panel and that's it!

At the end, you should get a screen similar to this:

![druid-step-17](https://github.com/user-attachments/assets/330b2629-9cbc-419c-b29c-99fdf8e6aaff)

##### Congratulations! :)

You can now start querying OpenTelemetry data! :)



