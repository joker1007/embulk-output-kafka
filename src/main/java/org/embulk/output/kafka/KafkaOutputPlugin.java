package org.embulk.output.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaOutputPlugin
        implements OutputPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("brokers")
        public List<String> getBrokers();

        @Config("schema_registry_url")
        @ConfigDefault("null")
        public Optional<String> getSchemaRegistryUrl();

        @Config("serialize_format")
        @ConfigDefault("\"json\"")
        public String getSerializeFormat();

        @Config("avsc_file")
        @ConfigDefault("null")
        public Optional<String> getAvscFile();

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

        KafkaProducer<Object, ObjectNode> producer = RecordProducerFactory.get(task, schema, task.getOtherProducerConfigs());

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
                            }
                            jsonNode.put(column.getName(), pageReader.getBoolean(column));
                        }

                        @Override
                        public void longColumn(Column column)
                        {
                            if (pageReader.isNull(column)) {
                                jsonNode.putNull(column.getName());
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
                            }
                            jsonNode.put(column.getName(), pageReader.getTimestamp(column).toEpochMilli());
                        }

                        @Override
                        public void jsonColumn(Column column)
                        {
                            if (pageReader.isNull(column)) {
                                jsonNode.putNull(column.getName());
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

                    ProducerRecord<Object, ObjectNode> producerRecord = new ProducerRecord<>("topic", key[0], jsonNode);
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
            }

            @Override
            public TaskReport commit()
            {
                return null;
            }
        };
    }
}
