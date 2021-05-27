package org.embulk.output.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.embulk.config.TaskReport;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.TransactionalPageOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public abstract class KafkaTransactionalPageOutput<P, T extends P> implements TransactionalPageOutput
{
    private static final Logger logger = LoggerFactory.getLogger(KafkaTransactionalPageOutput.class);

    private final KafkaProducer<Object, P> producer;
    private final PageReader pageReader;
    private final KafkaOutputColumnVisitor<T> columnVisitor;
    private final String topic;
    private final int taskIndex;
    private final boolean treatProducerExceptionAsError;
    private Exception lastProducerException;

    private final AtomicLong counter = new AtomicLong(0);
    private final AtomicLong recordLoggingCount = new AtomicLong(1);

    public KafkaTransactionalPageOutput(
            KafkaProducer<Object, P> producer,
            PageReader pageReader,
            KafkaOutputColumnVisitor<T> columnVisitor,
            String topic, int taskIndex, boolean treatProducerExceptionAsError)
    {
        this.producer = producer;
        this.pageReader = pageReader;
        this.columnVisitor = columnVisitor;
        this.topic = topic;
        this.taskIndex = taskIndex;
        this.treatProducerExceptionAsError = treatProducerExceptionAsError;
    }

    @Override
    public void add(Page page)
    {
        pageReader.setPage(page);
        while (pageReader.nextRecord()) {
            columnVisitor.reset();

            pageReader.getSchema().visitColumns(columnVisitor);

            Object recordKey = columnVisitor.getRecordKey();
            String targetTopic = columnVisitor.getTopicName() != null ? columnVisitor.getTopicName() : topic;
            T record = columnVisitor.getRecord();

            ProducerRecord<Object, P> producerRecord = new ProducerRecord<>(targetTopic, columnVisitor.getPartition(), recordKey, record);
            producer.send(producerRecord, (metadata, exception) -> {
                if (exception != null) {
                    lastProducerException = exception;
                    logger.error("produce error", exception);
                }

                logger.debug("sent record: {topic: {}, key: {}, value: {}, partition: {}}",
                        producerRecord.topic(),
                        producerRecord.key(),
                        producerRecord.value(),
                        producerRecord.partition());

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
        if (treatProducerExceptionAsError && lastProducerException != null) {
            throw new RuntimeException(lastProducerException);
        }
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
}
