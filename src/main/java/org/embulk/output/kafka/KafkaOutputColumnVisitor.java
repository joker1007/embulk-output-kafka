package org.embulk.output.kafka;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageReader;

public abstract class KafkaOutputColumnVisitor<T> implements ColumnVisitor
{
    private KafkaOutputPlugin.PluginTask task;
    PageReader pageReader;
    private String partitionColumnName;

    private Object recordKey = null;
    private String topicName = null;
    private Integer partition = null;

    KafkaOutputColumnVisitor(KafkaOutputPlugin.PluginTask task, PageReader pageReader)
    {
        this.task = task;
        this.pageReader = pageReader;
        this.partitionColumnName = task.getPartitionColumnName().orElse(null);
    }

    public abstract T getRecord();

    Object getRecordKey()
    {
        return recordKey;
    }

    private void setRecordKey(Column column, Object value)
    {
        if (task.getKeyColumnName().isPresent() && task.getKeyColumnName().get().equals(column.getName())) {
            recordKey = value;
        }
    }

    String getTopicName()
    {
        return topicName;
    }

    private void setTopicName(Column column, String value)
    {
        if (task.getTopicColumn().isPresent() && task.getTopicColumn().get().equals(column.getName())) {
            topicName = value;
        }
    }

    Integer getPartition()
    {
        return partition;
    }

    void reset()
    {
        this.recordKey = null;
        this.topicName = null;
        this.partition = null;
    }

    boolean isIgnoreColumn(Column column)
    {
        return task.getIgnoreColumns().stream().anyMatch(name -> name.equals(column.getName()));
    }

    @Override
    public void longColumn(Column column)
    {
        if (!pageReader.isNull(column)) {
            long value = pageReader.getLong(column);
            setRecordKey(column, value);

            if (partitionColumnName != null && partitionColumnName.equals(column.getName())) {
                partition = Long.valueOf(value).intValue();
            }
        }
    }

    @Override
    public void doubleColumn(Column column)
    {
        if (!pageReader.isNull(column)) {
            setRecordKey(column, pageReader.getDouble(column));
        }
    }

    @Override
    public void stringColumn(Column column)
    {
        if (!pageReader.isNull(column)) {
            setRecordKey(column, pageReader.getString(column));
            setTopicName(column, pageReader.getString(column));
        }
    }
}
