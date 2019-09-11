# Kafka output plugin for Embulk

## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: no
* **Cleanup supported**: yes

## Configuration

- **broker**: kafka broker host and port (array<string>, required)
- **topic**: target topic name (string, required)
- **topic_column**: use column value as target topic (string, default: `null`)
- **schema_registry_url**: Schema Registy URL that is needed for avro format (string, default: `null`)
- **serialize_format**: use column value as target topic (enum, required, `json` or `avro_with_schema_registry`)
- **avsc_file**: avro schema file path (string, default: `null`)
- **avsc**: inline avro schema config (json, default: `null`)
- **key_column_name**: use column value as record key (string, default: `null`, if this parameter is null, set random number as record key)
- **record_batch_size**: kafka producer record batch size (integer, default: `500`)
- **acks**: kafka producer require acks (string, default: `"1"`)
- **retries**: kafka producer max retry count (integer, default: `1`)
- **other_producer_configs**: other producer configs (json, default: `{}`)

If use `avro_with_schema_registry` format, following configs are required.

- **schema_registry_url**
- **avsc** or **avsc_file**

## Example

```yaml
in:
  type: file
  path_prefix: ./src/test/resources/in1
  parser:
    charset: UTF-8
    newline: CRLF
    type: csv
    delimiter: ','
    quote: '"'
    escape: '"'
    null_string: 'NULL'
    skip_header_lines: 1
    columns:
    - {name: 'id', type: string}
    - {name: 'int_item', type: long}
    - {name: 'varchar_item', type: string}

out:
  type: kafka
  topic: "json-topic"
  serialize_format: json
  brokers:
    - "localhost:9092"
```

```yaml
in:
  type: file
  path_prefix: ./src/test/resources/in_complex
  parser:
    charset: UTF-8
    newline: CRLF
    type: csv
    delimiter: "\t"
    quote: "\0"
    escape: "\0"
    null_string: 'NULL'
    skip_header_lines: 1
    columns:
    - {name: 'id', type: string}
    - {name: 'int_item', type: long}
    - {name: 'time', type: timestamp, format: "%Y-%m-%dT%H:%M:%S"}
    - {name: 'array', type: json}
    - {name: 'data', type: json}

out:
  type: kafka
  topic: "avro-complex-topic"
  acks: all
  retries: 3
  brokers:
    - "localhost:9092"
  schema_registry_url: "http://localhost:48081/"
  serialize_format: avro_with_schema_registry
  other_producer_configs:
    buffer.memory: "67108864"
  avsc:
    type: record
    name: ComplexRecord
    fields: [
      {name: "id", type: "string"},
      {name: "int_item", type: "long"},
      {name: "time", type: "long", logicalType: "timestamp-milli"},
      {name: "array", type: {type: "array", items: "long"}},
      {name: "data", type: {type: "record", name: "InnerData", fields: [
        {name: "hoge", type: "string"},
        {name: "aaa", type: ["null", "string"]},
        {name: "array", type: {type: "array", items: "long"}},
      ]}},
    ]
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
