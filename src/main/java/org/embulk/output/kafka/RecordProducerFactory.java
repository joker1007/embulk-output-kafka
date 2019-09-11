package org.embulk.output.kafka;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Schema;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class RecordProducerFactory
{
    public static KafkaProducer<Object, ObjectNode> get(KafkaOutputPlugin.PluginTask task, Schema schema, Map<String, String> configs)
    {
        Properties kafkaProps = new Properties();

        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, task.getBrokers());
        kafkaProps.put(ProducerConfig.ACKS_CONFIG, task.getAcks());
        kafkaProps.put(ProducerConfig.RETRIES_CONFIG, task.getRetries());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class);
        kafkaProps.put(ProducerConfig.BATCH_SIZE_CONFIG, task.getRecordBatchSize());

        configs.forEach(kafkaProps::setProperty);

        if (task.getKeyColumnName().isPresent()) {
            String keyColumnName = task.getKeyColumnName().get();
            Optional<Column> column = schema.getColumns().stream().filter(c -> c.getName().equals(keyColumnName)).findFirst();

            column.map(c -> {
                c.visit(new ColumnVisitor()
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
                return true;
            }).orElseGet(() -> {
                kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
                return true;
            });
        }
        else {
            kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        }

        return new KafkaProducer<>(kafkaProps);
    }
}
