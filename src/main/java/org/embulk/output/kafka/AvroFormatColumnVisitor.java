package org.embulk.output.kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class AvroFormatColumnVisitor extends KafkaOutputColumnVisitor
{
    private Schema avroSchema;
    public GenericRecord genericRecord;

    public AvroFormatColumnVisitor(KafkaOutputPlugin.PluginTask task, PageReader pageReader, Schema avroSchema, GenericRecord genericRecord)
    {
        super(task, pageReader);
        this.avroSchema = avroSchema;
        this.genericRecord = genericRecord;
    }

    @Override
    public void booleanColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            genericRecord.put(column.getName(), null);
            return;
        }

        genericRecord.put(column.getName(), pageReader.getBoolean(column));
    }

    @Override
    public void longColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            genericRecord.put(column.getName(), null);
            return;
        }

        genericRecord.put(column.getName(), pageReader.getLong(column));
        super.longColumn(column);
    }

    @Override
    public void doubleColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            genericRecord.put(column.getName(), null);
            return;
        }

        genericRecord.put(column.getName(), pageReader.getDouble(column));
        super.doubleColumn(column);
    }

    @Override
    public void stringColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            genericRecord.put(column.getName(), null);
            return;
        }

        genericRecord.put(column.getName(), pageReader.getString(column));
        super.stringColumn(column);
    }

    @Override
    public void timestampColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            genericRecord.put(column.getName(), null);
            return;
        }

        genericRecord.put(column.getName(), pageReader.getTimestamp(column).getInstant().toEpochMilli());
    }

    @Override
    public void jsonColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            genericRecord.put(column.getName(), null);
            return;
        }

        Value value = pageReader.getJson(column);
        try {
            Object avroValue = convertMsgPackValueToAvroValue(avroSchema.getField(column.getName()).schema(), value);
            genericRecord.put(column.getName(), avroValue);
        }
        catch (RuntimeException ex) {
            ex.printStackTrace();
        }
    }

    private Object convertMsgPackValueToAvroValue(org.apache.avro.Schema avroSchema, Value value)
    {
        switch (avroSchema.getType()) {
            case ARRAY:
                if (value.isArrayValue()) {
                    return value.asArrayValue().list().stream().map(item -> {
                        return convertMsgPackValueToAvroValue(avroSchema.getElementType(), item);
                    }).filter(Objects::nonNull).collect(Collectors.toList());
                }
                throw new RuntimeException(String.format("Schema mismatch: avro: %s, msgpack: %s", avroSchema.getType().getName(), value.getValueType().name()));
            case MAP:
                if (value.isMapValue()) {
                    Map<String, Object> map = new HashMap<>();
                    for (Map.Entry<Value, Value> entry : value.asMapValue().entrySet()) {
                        if (!entry.getValue().isNilValue()) {
                            map.put(entry.getKey().asStringValue().toString(), convertMsgPackValueToAvroValue(avroSchema.getValueType(), entry.getValue()));
                        }
                    }
                    return map;
                }
                throw new RuntimeException(String.format("Schema mismatch: avro: %s, msgpack: %s", avroSchema.getType().getName(), value.getValueType().name()));
            case RECORD:
                if (value.isMapValue()) {
                    GenericRecord record = new GenericData.Record(avroSchema);
                    Map<Value, Value> valueMap = value.asMapValue().map();
                    for (org.apache.avro.Schema.Field field : avroSchema.getFields()) {
                        Optional.ofNullable(valueMap.get(ValueFactory.newString(field.name()))).ifPresent(v -> {
                            record.put(field.name(), convertMsgPackValueToAvroValue(field.schema(), v));
                        });
                    }
                    return record;
                }
                throw new RuntimeException(String.format("Schema mismatch: avro: %s, msgpack: %s", avroSchema.getType().getName(), value.getValueType().name()));
            case LONG:
                if (value.isIntegerValue()) {
                    return value.asIntegerValue().toLong();
                }
                throw new RuntimeException(String.format("Schema mismatch: avro: %s, msgpack: %s", avroSchema.getType().getName(), value.getValueType().name()));
            case INT:
                if (value.isIntegerValue()) {
                    return value.asIntegerValue().toInt();
                }
                throw new RuntimeException(String.format("Schema mismatch: avro: %s, msgpack: %s", avroSchema.getType().getName(), value.getValueType().name()));
            case FLOAT:
                if (value.isFloatValue()) {
                    return value.asFloatValue().toFloat();
                }
                throw new RuntimeException(String.format("Schema mismatch: avro: %s, msgpack: %s", avroSchema.getType().getName(), value.getValueType().name()));
            case DOUBLE:
                if (value.isFloatValue()) {
                    return value.asFloatValue().toDouble();
                }
                throw new RuntimeException(String.format("Schema mismatch: avro: %s, msgpack: %s", avroSchema.getType().getName(), value.getValueType().name()));
            case BOOLEAN:
                if (value.isBooleanValue()) {
                    return value.asBooleanValue().getBoolean();
                }
                throw new RuntimeException(String.format("Schema mismatch: avro: %s, msgpack: %s", avroSchema.getType().getName(), value.getValueType().name()));
            case STRING:
            case ENUM:
                if (value.isStringValue()) {
                    return value.asStringValue().toString();
                }
                throw new RuntimeException(String.format("Schema mismatch: avro: %s, msgpack: %s", avroSchema.getType().getName(), value.getValueType().name()));
            case NULL:
                if (value.isNilValue()) {
                    return null;
                }
                throw new RuntimeException(String.format("Schema mismatch: avro: %s, msgpack: %s", avroSchema.getType().getName(), value.getValueType().name()));
            case UNION:
                for (org.apache.avro.Schema innerSchema : avroSchema.getTypes()) {
                    try {
                        return convertMsgPackValueToAvroValue(innerSchema, value);
                    }
                    catch (RuntimeException ignored) {
                    }
                }
                throw new RuntimeException(String.format("Schema mismatch: avro: %s, msgpack: %s", avroSchema.getType().getName(), value.getValueType().name()));
            case BYTES:
            case FIXED:
            default:
                throw new RuntimeException(String.format("Unsupported avro type %s", avroSchema.getType().getName()));
        }
    }
}
