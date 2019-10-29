package org.embulk.output.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;
import org.msgpack.value.Value;

import java.io.IOException;
import java.time.format.DateTimeFormatter;

public class JsonFormatColumnVisitor extends KafkaOutputColumnVisitor
{
    private ObjectMapper objectMapper;
    private ObjectNode jsonNode;

    private static DateTimeFormatter timestampFormatter = DateTimeFormatter.ISO_INSTANT;

    JsonFormatColumnVisitor(KafkaOutputPlugin.PluginTask task, PageReader pageReader, ObjectMapper objectMapper)
    {
        super(task, pageReader);
        this.objectMapper = objectMapper;
    }

    ObjectNode getJsonNode()
    {
        return jsonNode;
    }

    @Override
    void reset()
    {
        super.reset();
        this.jsonNode = objectMapper.createObjectNode();
    }

    @Override
    public void booleanColumn(Column column)
    {
        if (isIgnoreColumn(column)) {
            return;
        }

        if (pageReader.isNull(column)) {
            jsonNode.putNull(column.getName());
            return;
        }

        jsonNode.put(column.getName(), pageReader.getBoolean(column));
    }

    @Override
    public void longColumn(Column column)
    {
        super.longColumn(column);

        if (isIgnoreColumn(column)) {
            return;
        }

        if (pageReader.isNull(column)) {
            jsonNode.putNull(column.getName());
            return;
        }

        jsonNode.put(column.getName(), pageReader.getLong(column));
        super.longColumn(column);
    }

    @Override
    public void doubleColumn(Column column)
    {
        super.doubleColumn(column);

        if (isIgnoreColumn(column)) {
            return;
        }

        if (pageReader.isNull(column)) {
            jsonNode.putNull(column.getName());
            return;
        }

        jsonNode.put(column.getName(), pageReader.getDouble(column));
    }

    @Override
    public void stringColumn(Column column)
    {
        super.stringColumn(column);

        if (isIgnoreColumn(column)) {
            return;
        }

        if (pageReader.isNull(column)) {
            jsonNode.putNull(column.getName());
            return;
        }

        jsonNode.put(column.getName(), pageReader.getString(column));
    }

    @Override
    public void timestampColumn(Column column)
    {
        if (isIgnoreColumn(column)) {
            return;
        }

        if (pageReader.isNull(column)) {
            jsonNode.putNull(column.getName());
            return;
        }

        jsonNode.put(column.getName(), timestampFormatter.format(pageReader.getTimestamp(column).getInstant()));
    }

    @Override
    public void jsonColumn(Column column)
    {
        if (isIgnoreColumn(column)) {
            return;
        }

        if (pageReader.isNull(column)) {
            jsonNode.putNull(column.getName());
            return;
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
}
