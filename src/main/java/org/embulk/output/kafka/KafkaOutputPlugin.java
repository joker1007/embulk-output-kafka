package org.embulk.output.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.generic.GenericData;
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
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

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

        @Config("topic_column")
        @ConfigDefault("null")
        public Optional<String> getTopicColumn();

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
        PrimitiveIterator.OfLong randomLong = new Random().longs(1, Long.MAX_VALUE).iterator();
        AtomicInteger counter = new AtomicInteger(0);

        return new TransactionalPageOutput() {
            @Override
            public void add(Page page)
            {
                pageReader.setPage(page);
                while (pageReader.nextRecord()) {
                    JsonFormatColumnVisitor columnVisitor = new JsonFormatColumnVisitor(task, pageReader, objectMapper);

                    pageReader.getSchema().visitColumns(columnVisitor);

                    Object recordKey = columnVisitor.recordKey;
                    if (recordKey == null) {
                        recordKey = randomLong.next();
                    }

                    String targetTopic = columnVisitor.topicName != null ? columnVisitor.topicName : task.getTopic();
                    ProducerRecord<Object, ObjectNode> producerRecord = new ProducerRecord<>(targetTopic, recordKey, columnVisitor.jsonNode);
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
        final String[] topicName = new String[1];
        PrimitiveIterator.OfLong randomLong = new Random().longs(1, Long.MAX_VALUE).iterator();

        AtomicInteger counter = new AtomicInteger(0);

        final org.apache.avro.Schema finalAvroSchema = avroSchema;
        return new TransactionalPageOutput()
        {
            @Override
            public void add(Page page)
            {
                pageReader.setPage(page);
                while (pageReader.nextRecord()) {
                    AvroFormatColumnVisitor columnVisitor = new AvroFormatColumnVisitor(task, pageReader, finalAvroSchema, new GenericData.Record(finalAvroSchema));

                    pageReader.getSchema().visitColumns(columnVisitor);

                    Object recordKey = columnVisitor.recordKey;
                    if (recordKey == null) {
                        recordKey = randomLong.next();
                    }

                    String targetTopic = columnVisitor.topicName != null ? columnVisitor.topicName : task.getTopic();

                    ProducerRecord<Object, Object> producerRecord = new ProducerRecord<>(targetTopic, recordKey, columnVisitor.genericRecord);
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
