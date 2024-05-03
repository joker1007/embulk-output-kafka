package org.embulk.output.kafka;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.embulk.spi.PageReader;

public class AvroFormatTransactionalPageOutput
    extends KafkaTransactionalPageOutput<Object, GenericRecord> {
  public AvroFormatTransactionalPageOutput(
      KafkaProducer<Object, Object> producer,
      PageReader pageReader,
      KafkaOutputColumnVisitor<GenericRecord> columnVisitor,
      String topic,
      int taskIndex,
      boolean treatProducerExceptionAsError) {
    super(producer, pageReader, columnVisitor, topic, taskIndex, treatProducerExceptionAsError);
  }
}
;
