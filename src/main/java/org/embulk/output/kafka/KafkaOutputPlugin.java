package org.embulk.output.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
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
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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

        @Config("partition_column_name")
        @ConfigDefault("null")
        public Optional<String> getPartitionColumnName();

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

        @Config("ignore_columns")
        @ConfigDefault("[]")
        public List<String> getIgnoreColumns();

        @Config("value_subject_name_strategy")
        @ConfigDefault("null")
        public Optional<String> getValueSubjectNameStrategy();
    }

    private static ObjectMapper objectMapper = new ObjectMapper();
    private Logger logger = LoggerFactory.getLogger(getClass());

    private AdminClient getKafkaAdminClient(PluginTask task)
    {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, task.getBrokers());
        AdminClient adminClient = AdminClient.create(properties);
        return adminClient;
    }

    @Override
    public ConfigDiff transaction(ConfigSource config,
            Schema schema, int taskCount,
            Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        AdminClient adminClient = getKafkaAdminClient(task);
        DescribeTopicsResult result = adminClient.describeTopics(ImmutableList.of(task.getTopic()));
        try {
            if (result.all().get(30, TimeUnit.SECONDS).size() == 0) {
                throw new RuntimeException("target topic is not found");
            }
        }
        catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException("failed to connect kafka brokers");
        }

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
        AtomicLong counter = new AtomicLong(0);
        AtomicLong recordLoggingCount = new AtomicLong(1);

        return new TransactionalPageOutput() {
            private JsonFormatColumnVisitor columnVisitor = new JsonFormatColumnVisitor(task, pageReader, objectMapper);

            @Override
            public void add(Page page)
            {
                pageReader.setPage(page);
                while (pageReader.nextRecord()) {
                    columnVisitor.reset();

                    pageReader.getSchema().visitColumns(columnVisitor);

                    Object recordKey = columnVisitor.getRecordKey();
                    if (recordKey == null) {
                        recordKey = randomLong.next();
                    }

                    String targetTopic = columnVisitor.getTopicName() != null ? columnVisitor.getTopicName() : task.getTopic();
                    ProducerRecord<Object, ObjectNode> producerRecord = new ProducerRecord<>(targetTopic, recordKey, columnVisitor.getJsonNode());
                    producer.send(producerRecord, (metadata, exception) -> {
                        if (exception != null) {
                            logger.error("produce error", exception);
                        }

                        logger.debug("sent record: {key: {}, value: {}}", producerRecord.key(), producerRecord.value());

                        long current = counter.incrementAndGet();
                        if (current >= recordLoggingCount.get()) {
                            logger.info("[task-{}] Producer sent {} records", String.format("%04d", taskIndex), current);
                            recordLoggingCount.set(recordLoggingCount.get() * 2);
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

        PrimitiveIterator.OfLong randomLong = new Random().longs(1, Long.MAX_VALUE).iterator();

        AtomicInteger counter = new AtomicInteger(0);
        AtomicInteger recordLoggingCount = new AtomicInteger(1);

        final org.apache.avro.Schema finalAvroSchema = avroSchema;
        return new TransactionalPageOutput()
        {
            private AvroFormatColumnVisitor columnVisitor = new AvroFormatColumnVisitor(task, pageReader, finalAvroSchema);

            @Override
            public void add(Page page)
            {
                pageReader.setPage(page);
                while (pageReader.nextRecord()) {
                    columnVisitor.reset();

                    pageReader.getSchema().visitColumns(columnVisitor);

                    Object recordKey = columnVisitor.getRecordKey();
                    if (recordKey == null) {
                        recordKey = randomLong.next();
                    }

                    String targetTopic = columnVisitor.getTopicName() != null ? columnVisitor.getTopicName() : task.getTopic();

                    ProducerRecord<Object, Object> producerRecord = new ProducerRecord<>(targetTopic, columnVisitor.getPartition(), recordKey, columnVisitor.getGenericRecord());
                    producer.send(producerRecord, (metadata, exception) -> {
                        if (exception != null) {
                            logger.error("produce error", exception);
                        }

                        logger.debug("sent record: {key: {}, value: {}}", producerRecord.key(), producerRecord.value());

                        int current = counter.incrementAndGet();
                        if (current >= recordLoggingCount.get()) {
                            logger.info("[task-{}] Producer sent {} records", String.format("%04d", taskIndex), current);
                            recordLoggingCount.set(recordLoggingCount.get() * 2);
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
