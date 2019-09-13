package org.embulk.output.kafka;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageReader;

public abstract class KafkaOutputColumnVisitor implements ColumnVisitor
{
    KafkaOutputPlugin.PluginTask task;
    PageReader pageReader;

    public Object recordKey = null;
    public String topicName = null;

    public KafkaOutputColumnVisitor(KafkaOutputPlugin.PluginTask task, PageReader pageReader)
    {
        this.task = task;
        this.pageReader = pageReader;
    }

    void setRecordKey(Column column, Object value)
    {
        if (task.getKeyColumnName().isPresent() && task.getKeyColumnName().get().equals(column.getName())) {
            recordKey = value;
        }
    }

    void setTopicName(Column column, String value)
    {
        if (task.getTopicColumn().isPresent() && task.getTopicColumn().get().equals(column.getName())) {
            topicName = value;
        }
    }

    boolean isIgnoreColumn(Column column)
    {
        return task.getIgnoreColumns().stream().anyMatch(name -> name.equals(column.getName()));
    }

    @Override
    public void longColumn(Column column)
    {
        if (!pageReader.isNull(column)) {
            setRecordKey(column, pageReader.getLong(column));
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
