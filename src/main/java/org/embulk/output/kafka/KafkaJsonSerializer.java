package org.embulk.output.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serializer;

public class KafkaJsonSerializer implements Serializer<ObjectNode> {
  private static ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public byte[] serialize(String topic, ObjectNode data) {
    if (data == null) {
      return null;
    }

    try {
      return objectMapper.writeValueAsBytes(data);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
