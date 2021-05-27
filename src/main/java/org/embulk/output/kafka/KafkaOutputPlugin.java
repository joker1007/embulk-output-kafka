package org.embulk.output.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.TaskMapper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

        @Config("subject_name")
        @ConfigDefault("null")
        public Optional<String> getSubjectName();

        @Config("key_column_name")
        @ConfigDefault("null")
        public Optional<String> getKeyColumnName();

        @Config("treat_producer_exception_as_error")
        @ConfigDefault("false")
        public boolean getTreatProducerExceptionAsError();

        @Config("partition_column_name")
        @ConfigDefault("null")
        public Optional<String> getPartitionColumnName();

        @Config("record_batch_size")
        @ConfigDefault("16384")
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

        @Config("column_for_deletion")
        @ConfigDefault("null")
        public Optional<String> getColumnForDeletion();
    }

    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder()
        .addDefaultModules().build();

    private static ObjectMapper objectMapper = new ObjectMapper();

    private static final int SCHEMA_REGISTRY_IDENTITY_MAP_CAPACITY = 1000;

    private AdminClient getKafkaAdminClient(PluginTask task)
    {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, task.getBrokers());
        return AdminClient.create(properties);
    }

    @Override
    public ConfigDiff transaction(ConfigSource config,
            Schema schema, int taskCount,
            Control control)
    {
        final ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
        final PluginTask task = configMapper.map(config, PluginTask.class);
        AdminClient adminClient = getKafkaAdminClient(task);
        List<String> topics = new ArrayList<>();
        topics.add(task.getTopic());
        DescribeTopicsResult result = adminClient.describeTopics(topics);
        try {
            if (result.all().get(30, TimeUnit.SECONDS).size() == 0) {
                throw new RuntimeException("target topic is not found");
            }
        }
        catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException("failed to connect kafka brokers");
        }

        control.run(task.toTaskSource());
        return CONFIG_MAPPER_FACTORY.newConfigDiff();
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
        final TaskMapper taskMapper = CONFIG_MAPPER_FACTORY.createTaskMapper();
        PluginTask task = taskMapper.map(taskSource, PluginTask.class);

        switch (task.getRecordSerializeFormat()) {
            case JSON:
                return buildPageOutputForJson(task, schema, taskIndex);
            case AVRO_WITH_SCHEMA_REGISTRY:
                return buildPageOutputForAvroWithSchemaRegistry(task, schema, taskIndex);
            default:
                throw new ConfigException("Unknown serialize format");
        }
    }

    private TransactionalPageOutput buildPageOutputForJson(PluginTask task, Schema schema, int taskIndex)
    {
        KafkaProducer<Object, ObjectNode> producer = RecordProducerFactory.getForJson(task, schema, task.getOtherProducerConfigs());
        PageReader pageReader = new PageReader(schema);
        KafkaOutputColumnVisitor<ObjectNode> columnVisitor = new JsonFormatColumnVisitor(task, pageReader, objectMapper);

        return new JsonFormatTransactionalPageOutput(producer, pageReader, columnVisitor, task.getTopic(), taskIndex, task.getTreatProducerExceptionAsError());
    }

    private TransactionalPageOutput buildPageOutputForAvroWithSchemaRegistry(PluginTask task, Schema schema, int taskIndex)
    {
        KafkaProducer<Object, Object> producer = RecordProducerFactory.getForAvroWithSchemaRegistry(task, schema, task.getOtherProducerConfigs());
        PageReader pageReader = new PageReader(schema);
        org.apache.avro.Schema avroSchema = getAvroSchema(task);
        AvroFormatColumnVisitor avroFormatColumnVisitor = new AvroFormatColumnVisitor(task, pageReader, avroSchema);

        return new AvroFormatTransactionalPageOutput(producer, pageReader, avroFormatColumnVisitor, task.getTopic(), taskIndex, task.getTreatProducerExceptionAsError());
    }

    private org.apache.avro.Schema getAvroSchema(PluginTask task)
    {
        org.apache.avro.Schema avroSchema;
        if (!task.getSchemaRegistryUrl().isPresent()) {
            throw new ConfigException("avro_with_schema_registry format needs schema_registry_url");
        }

        if (task.getAvsc().isPresent() && task.getAvscFile().isPresent()) {
            throw new ConfigException("avro_with_schema_registry format needs either one of avsc and avsc_file");
        }

        if (task.getAvsc().isPresent()) {
            avroSchema = new org.apache.avro.Schema.Parser().parse(task.getAvsc().get().toString());
            return avroSchema;
        }
        if (task.getAvscFile().isPresent()) {
            try {
                avroSchema = new org.apache.avro.Schema.Parser().parse(task.getAvscFile().get());
                return avroSchema;
            }
            catch (IOException e) {
                e.printStackTrace();
                throw new ConfigException("avsc_file cannot read");
            }
        }

        SchemaRegistryClient schemaRegistryClient = getSchemaRegistryClient(task.getSchemaRegistryUrl().get());
        String subjectName = task.getSubjectName().orElseGet(() -> {
            SubjectNameStrategy subjectNameStrategy = new TopicNameStrategy();
            return subjectNameStrategy.subjectName(task.getTopic(), false, null);
        });
        try {
            String schema = schemaRegistryClient.getLatestSchemaMetadata(subjectName).getSchema();
            avroSchema = new org.apache.avro.Schema.Parser().parse(schema);
            return avroSchema;
        }
        catch (IOException | RestClientException e) {
            throw new ConfigException("cannot fetch latest schema from schema registry.", e);
        }
    }

    private static final String MOCK_SCHEMA_REGISTRY_PREFIX = "mock://";
    private SchemaRegistryClient getSchemaRegistryClient(String url)
    {
        if (url.startsWith(MOCK_SCHEMA_REGISTRY_PREFIX)) {
            String mockScope = url.substring(MOCK_SCHEMA_REGISTRY_PREFIX.length());
            return MockSchemaRegistry.getClientForScope(mockScope);
        }
        else {
            return new CachedSchemaRegistryClient(url, SCHEMA_REGISTRY_IDENTITY_MAP_CAPACITY);
        }
    }
}
