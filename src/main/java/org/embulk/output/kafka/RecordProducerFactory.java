package org.embulk.output.kafka;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.embulk.config.ConfigException;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Schema;

import java.util.Map;
import java.util.Properties;

class RecordProducerFactory
{
    private RecordProducerFactory() {}

    private static Properties buildProperties(KafkaOutputPlugin.PluginTask task, Schema schema, Map<String, String> configs)
    {
        Properties kafkaProps = new Properties();

        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, task.getBrokers());
        kafkaProps.put(ProducerConfig.ACKS_CONFIG, task.getAcks());
        kafkaProps.put(ProducerConfig.RETRIES_CONFIG, task.getRetries());
        kafkaProps.put(ProducerConfig.BATCH_SIZE_CONFIG, task.getRecordBatchSize());

        configs.forEach(kafkaProps::setProperty);

        if (task.getKeyColumnName().isPresent()) {
            String keyColumnName = task.getKeyColumnName().get();
            Column column = schema.getColumns().stream()
                    .filter(c -> c.getName().equals(keyColumnName))
                    .findFirst()
                    .orElseThrow(() -> new ConfigException("key column is not found"));

            column.visit(new ColumnVisitor()
            {
                @Override
                public void booleanColumn(Column column)
                {
                    throw new RuntimeException("boolean column is not supported for key_column_name");
                }

                @Override
                public void longColumn(Column column)
                {
                    kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
                }

                @Override
                public void doubleColumn(Column column)
                {
                    kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, DoubleSerializer.class);
                }

                @Override
                public void stringColumn(Column column)
                {
                    kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                }

                @Override
                public void timestampColumn(Column column)
                {
                    throw new RuntimeException("timestamp column is not supported for key_column_name");
                }

                @Override
                public void jsonColumn(Column column)
                {
                    throw new RuntimeException("json column is not supported for key_column_name");
                }
            });
        }
        else {
            kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        }

        return kafkaProps;
    }

    static KafkaProducer<Object, ObjectNode> getForJson(KafkaOutputPlugin.PluginTask task, Schema schema, Map<String, String> configs)
    {
        return new KafkaProducer<>(buildProperties(task, schema, configs), null, new KafkaJsonSerializer());
    }

    static KafkaProducer<Object, Object> getForAvroWithSchemaRegistry(KafkaOutputPlugin.PluginTask task, Schema schema, Map<String, String> configs)
    {
        KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer();
        String schemaRegistryUrl = task.getSchemaRegistryUrl().orElseThrow(() -> new ConfigException("avro_with_schema_registry format needs schema_registry_url"));

        Map<String, String> avroSerializerConfigs = ImmutableMap.<String, String>builder()
                .put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl)
                .build();
        kafkaAvroSerializer.configure(avroSerializerConfigs, false);

        return new KafkaProducer<>(buildProperties(task, schema, configs), null, kafkaAvroSerializer);
    }
}
