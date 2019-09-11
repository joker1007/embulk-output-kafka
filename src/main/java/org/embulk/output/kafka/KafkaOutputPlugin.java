package org.embulk.output.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class KafkaOutputPlugin
        implements OutputPlugin
{
    public enum RecordSerializeFormat
    {
        JSON,
        AVRO_WITH_SCHEMA_REGISTRY;

        @JsonValue
        public String toString()
        {
            return name().toLowerCase(Locale.ENGLISH);
        }

        @JsonCreator
        public static RecordSerializeFormat ofString(String name)
        {
            switch (name.toLowerCase(Locale.ENGLISH)) {
                case "json":
                    return JSON;
                case "avro_with_schema_registry":
                    return AVRO_WITH_SCHEMA_REGISTRY;
                default:
            }

            throw new ConfigException(String.format("Unknown serialize format '%s'. Supported modes are json, avro_with_schema_registry", name));
        }
    }

    public interface PluginTask
            extends Task
    {
        @Config("brokers")
        public List<String> getBrokers();

        @Config("topic")
        public String getTopic();

        @Config("schema_registry_url")
        @ConfigDefault("null")
        public Optional<String> getSchemaRegistryUrl();

        @Config("serialize_format")
        @ConfigDefault("\"json\"")
        public RecordSerializeFormat getRecordSerializeFormat();

        @Config("avsc_file")
        @ConfigDefault("null")
        public Optional<File> getAvscFile();

        @Config("avsc")
        @ConfigDefault("null")
        public Optional<ObjectNode> getAvsc();

        @Config("key_column_name")
        @ConfigDefault("null")
        public Optional<String> getKeyColumnName();

        @Config("record_batch_size")
        @ConfigDefault("1000")
        public int getRecordBatchSize();

        @Config("acks")
        @ConfigDefault("\"1\"")
        public String getAcks();

        @Config("retries")
        @ConfigDefault("1")
        public int getRetries();

        @Config("other_producer_configs")
        @ConfigDefault("{}")
        public Map<String, String> getOtherProducerConfigs();
    }

    private static ObjectMapper objectMapper = new ObjectMapper();
    private Logger logger = LoggerFactory.getLogger(getClass());
    private int recordLoggingCount = 1;

    @Override
    public ConfigDiff transaction(ConfigSource config,
            Schema schema, int taskCount,
            Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        // retryable (idempotent) output:
        // return resume(task.dump(), schema, taskCount, control);

        // non-retryable (non-idempotent) output:
        control.run(task.dump());
        return Exec.newConfigDiff();
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            Control control)
    {
        throw new UnsupportedOperationException("kafka output plugin does not support resuming");
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TransactionalPageOutput open(TaskSource taskSource, Schema schema, int taskIndex)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        switch (task.getRecordSerializeFormat()) {
            case JSON:
                return buildPageOutputForJson(task, schema, taskIndex);
            case AVRO_WITH_SCHEMA_REGISTRY:
                return buildPageOutputForAvroWithSchemaRegistry(task, schema, taskIndex);
            default:
                throw new ConfigException("Unknow serialize format");
        }
    }

    private TransactionalPageOutput buildPageOutputForJson(PluginTask task, Schema schema, int taskIndex)
    {
        KafkaProducer<Object, ObjectNode> producer = RecordProducerFactory.getForJson(task, schema, task.getOtherProducerConfigs());

        PageReader pageReader = new PageReader(schema);

        final Object[] key = new Object[1];
        PrimitiveIterator.OfLong randomLong = new Random().longs(1, Long.MAX_VALUE).iterator();

        AtomicInteger counter = new AtomicInteger(0);

        return new TransactionalPageOutput() {
            @Override
            public void add(Page page)
            {
                ObjectNode jsonNode = objectMapper.createObjectNode();
                pageReader.setPage(page);
                while (pageReader.nextRecord()) {
                    key[0] = null;

                    pageReader.getSchema().visitColumns(new ColumnVisitor()
                    {
                        @Override
                        public void booleanColumn(Column column)
                        {
                            if (pageReader.isNull(column)) {
                                jsonNode.putNull(column.getName());
                                return;
                            }

                            jsonNode.put(column.getName(), pageReader.getBoolean(column));
                        }

                        @Override
                        public void longColumn(Column column)
                        {
                            if (pageReader.isNull(column)) {
                                jsonNode.putNull(column.getName());
                                return;
                            }

                            jsonNode.put(column.getName(), pageReader.getLong(column));
                            if (task.getKeyColumnName().isPresent() && task.getKeyColumnName().get().equals(column.getName())) {
                                key[0] = pageReader.getLong(column);
                            }
                        }

                        @Override
                        public void doubleColumn(Column column)
                        {
                            if (pageReader.isNull(column)) {
                                jsonNode.putNull(column.getName());
                                return;
                            }

                            jsonNode.put(column.getName(), pageReader.getDouble(column));
                            if (task.getKeyColumnName().isPresent() && task.getKeyColumnName().get().equals(column.getName())) {
                                key[0] = pageReader.getDouble(column);
                            }
                        }

                        @Override
                        public void stringColumn(Column column)
                        {
                            if (pageReader.isNull(column)) {
                                jsonNode.putNull(column.getName());
                                return;
                            }

                            jsonNode.put(column.getName(), pageReader.getString(column));
                            if (task.getKeyColumnName().isPresent() && task.getKeyColumnName().get().equals(column.getName())) {
                                key[0] = pageReader.getString(column);
                            }
                        }

                        @Override
                        public void timestampColumn(Column column)
                        {
                            if (pageReader.isNull(column)) {
                                jsonNode.putNull(column.getName());
                                return;
                            }

                            jsonNode.put(column.getName(), pageReader.getTimestamp(column).toEpochMilli());
                        }

                        @Override
                        public void jsonColumn(Column column)
                        {
                            if (pageReader.isNull(column)) {
                                jsonNode.putNull(column.getName());
                                return;
                            }

                            Value value = pageReader.getJson(column);
                            JsonNode json;
                            try {
                                json = objectMapper.readTree(value.toJson());
                            }
                            catch (IOException e) {
                                return;
                            }
                            jsonNode.set(column.getName(), json);
                        }
                    });

                    if (key[0] == null) {
                        key[0] = randomLong.next();
                    }

                    ProducerRecord<Object, ObjectNode> producerRecord = new ProducerRecord<>(task.getTopic(), key[0], jsonNode);
                    producer.send(producerRecord, (metadata, exception) -> {
                        if (exception != null) {
                            logger.error("produce error", exception);
                        }

                        logger.debug("sent record: {key: {}, value: {}}", producerRecord.key(), producerRecord.value());

                        int current = counter.incrementAndGet();
                        if (current >= recordLoggingCount) {
                            logger.info("[task-{}] Producer sent {} records", String.format("%04d", taskIndex), current);
                            recordLoggingCount = recordLoggingCount * 2;
                        }
                    });
                }
            }

            @Override
            public void finish()
            {
                producer.flush();
            }

            @Override
            public void close()
            {
                producer.close();
            }

            @Override
            public void abort()
            {
                producer.flush();
                producer.close();
            }

            @Override
            public TaskReport commit()
            {
                return null;
            }
        };
    }

    private TransactionalPageOutput buildPageOutputForAvroWithSchemaRegistry(PluginTask task, Schema schema, int taskIndex)
    {
        KafkaProducer<Object, Object> producer = RecordProducerFactory.getForAvroWithSchemaRegistry(task, schema, task.getOtherProducerConfigs());

        PageReader pageReader = new PageReader(schema);

        org.apache.avro.Schema avroSchema = null;
        if (!task.getAvsc().isPresent() && !task.getAvscFile().isPresent() || task.getAvsc().isPresent() == task.getAvscFile().isPresent()) {
            throw new ConfigException("avro_with_schema_registry format needs either one of avsc and avsc_file");
        }
        if (task.getAvsc().isPresent()) {
            avroSchema = new org.apache.avro.Schema.Parser().parse(task.getAvsc().get().toString());
        }
        if (task.getAvscFile().isPresent()) {
            try {
                avroSchema = new org.apache.avro.Schema.Parser().parse(task.getAvscFile().get());
            }
            catch (IOException e) {
                e.printStackTrace();
                throw new ConfigException("avsc_file cannot read");
            }
        }

        final Object[] key = new Object[1];
        PrimitiveIterator.OfLong randomLong = new Random().longs(1, Long.MAX_VALUE).iterator();

        AtomicInteger counter = new AtomicInteger(0);

        final org.apache.avro.Schema finalAvroSchema = avroSchema;
        return new TransactionalPageOutput()
        {
            @Override
            public void add(Page page)
            {
                GenericRecord genericRecord = new GenericData.Record(finalAvroSchema);

                pageReader.setPage(page);
                while (pageReader.nextRecord()) {
                    key[0] = null;

                    pageReader.getSchema().visitColumns(new ColumnVisitor()
                    {
                        @Override
                        public void booleanColumn(Column column)
                        {
                            if (pageReader.isNull(column)) {
                                genericRecord.put(column.getName(), null);
                                return;
                            }

                            genericRecord.put(column.getName(), pageReader.getBoolean(column));
                        }

                        @Override
                        public void longColumn(Column column)
                        {
                            if (pageReader.isNull(column)) {
                                genericRecord.put(column.getName(), null);
                                return;
                            }

                            genericRecord.put(column.getName(), pageReader.getLong(column));
                        }

                        @Override
                        public void doubleColumn(Column column)
                        {
                            if (pageReader.isNull(column)) {
                                genericRecord.put(column.getName(), null);
                                return;
                            }

                            genericRecord.put(column.getName(), pageReader.getDouble(column));
                        }

                        @Override
                        public void stringColumn(Column column)
                        {
                            if (pageReader.isNull(column)) {
                                genericRecord.put(column.getName(), null);
                                return;
                            }

                            genericRecord.put(column.getName(), pageReader.getString(column));
                        }

                        @Override
                        public void timestampColumn(Column column)
                        {
                            if (pageReader.isNull(column)) {
                                genericRecord.put(column.getName(), null);
                                return;
                            }

                            genericRecord.put(column.getName(), pageReader.getTimestamp(column).getInstant().toEpochMilli());
                        }

                        @Override
                        public void jsonColumn(Column column)
                        {
                            if (pageReader.isNull(column)) {
                                genericRecord.put(column.getName(), null);
                                return;
                            }

                            Value value = pageReader.getJson(column);
                            try {
                                Object avroValue = convertMsgPackValueToAvroValue(finalAvroSchema.getField(column.getName()).schema(), value);
                                genericRecord.put(column.getName(), avroValue);
                            }
                            catch (RuntimeException ex) {
                                ex.printStackTrace();
                            }
                        }

                        private Object convertMsgPackValueToAvroValue(org.apache.avro.Schema avroSchema, Value value)
                        {
                            switch (avroSchema.getType()) {
                                case ARRAY:
                                    if (value.isArrayValue()) {
                                        return value.asArrayValue().list().stream().map(item -> {
                                            return convertMsgPackValueToAvroValue(avroSchema.getElementType(), item);
                                        }).filter(Objects::nonNull).collect(Collectors.toList());
                                    }
                                    throw new RuntimeException(String.format("Schema mismatch: avro: %s, msgpack: %s", avroSchema.getType().getName(), value.getValueType().name()));
                                case MAP:
                                    if (value.isMapValue()) {
                                        Map<String, Object> map = new HashMap<>();
                                        for (Map.Entry<Value, Value> entry : value.asMapValue().entrySet()) {
                                            if (!entry.getValue().isNilValue()) {
                                                map.put(entry.getKey().asStringValue().toString(), convertMsgPackValueToAvroValue(avroSchema.getValueType(), entry.getValue()));
                                            }
                                        }
                                        return map;
                                    }
                                    throw new RuntimeException(String.format("Schema mismatch: avro: %s, msgpack: %s", avroSchema.getType().getName(), value.getValueType().name()));
                                case RECORD:
                                    if (value.isMapValue()) {
                                        GenericRecord record = new GenericData.Record(avroSchema);
                                        Map<Value, Value> valueMap = value.asMapValue().map();
                                        for (org.apache.avro.Schema.Field field : avroSchema.getFields()) {
                                            Optional.ofNullable(valueMap.get(ValueFactory.newString(field.name()))).ifPresent(v -> {
                                                record.put(field.name(), convertMsgPackValueToAvroValue(field.schema(), v));
                                            });
                                        }
                                        return record;
                                    }
                                    throw new RuntimeException(String.format("Schema mismatch: avro: %s, msgpack: %s", avroSchema.getType().getName(), value.getValueType().name()));
                                case LONG:
                                    if (value.isIntegerValue()) {
                                        return value.asIntegerValue().toLong();
                                    }
                                    throw new RuntimeException(String.format("Schema mismatch: avro: %s, msgpack: %s", avroSchema.getType().getName(), value.getValueType().name()));
                                case INT:
                                    if (value.isIntegerValue()) {
                                        return value.asIntegerValue().toInt();
                                    }
                                    throw new RuntimeException(String.format("Schema mismatch: avro: %s, msgpack: %s", avroSchema.getType().getName(), value.getValueType().name()));
                                case FLOAT:
                                    if (value.isFloatValue()) {
                                        return value.asFloatValue().toFloat();
                                    }
                                    throw new RuntimeException(String.format("Schema mismatch: avro: %s, msgpack: %s", avroSchema.getType().getName(), value.getValueType().name()));
                                case DOUBLE:
                                    if (value.isFloatValue()) {
                                        return value.asFloatValue().toDouble();
                                    }
                                    throw new RuntimeException(String.format("Schema mismatch: avro: %s, msgpack: %s", avroSchema.getType().getName(), value.getValueType().name()));
                                case BOOLEAN:
                                    if (value.isBooleanValue()) {
                                        return value.asBooleanValue().getBoolean();
                                    }
                                    throw new RuntimeException(String.format("Schema mismatch: avro: %s, msgpack: %s", avroSchema.getType().getName(), value.getValueType().name()));
                                case STRING:
                                case ENUM:
                                    if (value.isStringValue()) {
                                        return value.asStringValue().toString();
                                    }
                                    throw new RuntimeException(String.format("Schema mismatch: avro: %s, msgpack: %s", avroSchema.getType().getName(), value.getValueType().name()));
                                case NULL:
                                    if (value.isNilValue()) {
                                        return null;
                                    }
                                    throw new RuntimeException(String.format("Schema mismatch: avro: %s, msgpack: %s", avroSchema.getType().getName(), value.getValueType().name()));
                                case UNION:
                                    for (org.apache.avro.Schema innerSchema : avroSchema.getTypes()) {
                                        try {
                                            return convertMsgPackValueToAvroValue(innerSchema, value);
                                        }
                                        catch (RuntimeException ignored) {
                                        }
                                    }
                                    throw new RuntimeException(String.format("Schema mismatch: avro: %s, msgpack: %s", avroSchema.getType().getName(), value.getValueType().name()));
                                case BYTES:
                                case FIXED:
                                default:
                                    throw new RuntimeException(String.format("Unsupported avro type %s", avroSchema.getType().getName()));
                            }
                        }
                    });

                    if (key[0] == null) {
                        key[0] = randomLong.next();
                    }

                    ProducerRecord<Object, Object> producerRecord = new ProducerRecord<>(task.getTopic(), key[0], genericRecord);
                    producer.send(producerRecord, (metadata, exception) -> {
                        if (exception != null) {
                            logger.error("produce error", exception);
                        }

                        logger.debug("sent record: {key: {}, value: {}}", producerRecord.key(), producerRecord.value());

                        int current = counter.incrementAndGet();
                        if (current >= recordLoggingCount) {
                            logger.info("[task-{}] Producer sent {} records", String.format("%04d", taskIndex), current);
                            recordLoggingCount = recordLoggingCount * 2;
                        }
                    });
                }
            }

            @Override
            public void finish()
            {
                producer.flush();
            }

            @Override
            public void close()
            {
                producer.close();
            }

            @Override
            public void abort()
            {
                producer.flush();
                producer.close();
            }

            @Override
            public TaskReport commit()
            {
                return null;
            }
        };
    }
}
