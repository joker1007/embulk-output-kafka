package org.embulk.output.kafka;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.embulk.spi.PageReader;

public class JsonFormatTransactionalPageOutput extends KafkaTransactionalPageOutput<ObjectNode, ObjectNode>
{
    public JsonFormatTransactionalPageOutput(KafkaProducer<Object, ObjectNode> producer, PageReader pageReader, KafkaOutputColumnVisitor<ObjectNode> columnVisitor, String topic, int taskIndex)
    {
        super(producer, pageReader, columnVisitor, topic, taskIndex);
    }
};
