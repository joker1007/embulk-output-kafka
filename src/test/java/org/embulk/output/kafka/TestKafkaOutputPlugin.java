package org.embulk.output.kafka;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.embulk.config.ConfigSource;
import org.embulk.input.file.LocalFileInputPlugin;
import org.embulk.parser.csv.CsvParserPlugin;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.ParserPlugin;
import org.embulk.test.TestingEmbulk;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class TestKafkaOutputPlugin {
  @Rule
  public KafkaContainer kafka =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

  @Rule
  public TestingEmbulk embulk =
      TestingEmbulk.builder()
          .registerPlugin(FileInputPlugin.class, "file", LocalFileInputPlugin.class)
          .registerPlugin(ParserPlugin.class, "csv", CsvParserPlugin.class)
          .registerPlugin(OutputPlugin.class, "kafka", KafkaOutputPlugin.class)
          .build();

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private AdminClient getAdminClient() {
    return AdminClient.create(
        ImmutableMap.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()));
  }

  @Before
  public void setUp() {
    AdminClient adminClient = getAdminClient();
    Collection<NewTopic> topics =
        ImmutableList.of(
            new NewTopic("json-topic", 8, (short) 1),
            new NewTopic("json-complex-topic", 8, (short) 1),
            new NewTopic("avro-simple-topic", 8, (short) 1),
            new NewTopic("avro-complex-topic", 8, (short) 1));
    adminClient.createTopics(topics);
    adminClient.close();
  }

  @After
  public void tearDown() {
    AdminClient adminClient = getAdminClient();
    adminClient.deleteTopics(
        ImmutableList.of(
            "json-topic", "json-complex-topic", "avro-simple-topic", "avro-complex-topic"));
    adminClient.close();
  }

  private void setBootstrapServer(ConfigSource configSource) {
    configSource.set(
        "brokers", ImmutableList.of(kafka.getHost() + ":" + kafka.getFirstMappedPort()));
  }

  private <K, V> List<ConsumerRecord<K, V>> consumeAllRecordsFromTopic(
      String topic,
      Class<? extends Deserializer<K>> keyDeserializer,
      Class<? extends Deserializer<V>> valueDeserializer) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    try (KafkaConsumer<K, V> consumer = new KafkaConsumer<>(props)) {
      consumer.assign(
          IntStream.rangeClosed(0, 7)
              .mapToObj(i -> new TopicPartition(topic, i))
              .collect(Collectors.toList()));
      List<ConsumerRecord<K, V>> records = new ArrayList<>();
      consumer.poll(Duration.ofSeconds(5)).forEach(records::add);
      return records;
    }
  }

  @Test
  public void testSimpleJson() throws IOException {
    ConfigSource configSource = embulk.loadYamlResource("config_simple.yml");
    setBootstrapServer(configSource);
    embulk.runOutput(
        configSource, Paths.get(Resources.getResource("org/embulk/test/in1.csv").getPath()));

    List<ConsumerRecord<String, String>> consumerRecords =
        consumeAllRecordsFromTopic(
            "json-topic", StringDeserializer.class, StringDeserializer.class);

    assertEquals(3, consumerRecords.size());
    List<JsonNode> deserializedRecords = new ArrayList<>();
    for (ConsumerRecord<String, String> record : consumerRecords) {
      deserializedRecords.add(objectMapper.readTree(record.value()));
    }
    List<String> ids =
        deserializedRecords.stream().map(r -> r.get("id").asText()).collect(Collectors.toList());
    List<Integer> intItems =
        deserializedRecords.stream()
            .map(r -> r.get("int_item").asInt())
            .collect(Collectors.toList());
    List<String> varcharItems =
        deserializedRecords.stream()
            .map(r -> r.get("varchar_item").asText())
            .collect(Collectors.toList());

    assertThat(ids, hasItem("A001"));
    assertThat(ids, hasItem("A002"));
    assertThat(ids, hasItem("A003"));
    assertThat(intItems, hasItem(1));
    assertThat(intItems, hasItem(2));
    assertThat(intItems, hasItem(3));
    assertThat(varcharItems, hasItem("a"));
    assertThat(varcharItems, hasItem("b"));
    assertThat(varcharItems, hasItem("c"));
  }

  @Test
  public void testComplexJson() throws IOException {
    ConfigSource configSource = embulk.loadYamlResource("config_complex.yml");
    setBootstrapServer(configSource);

    embulk.runOutput(
        configSource, Paths.get(Resources.getResource("org/embulk/test/in_complex.csv").getPath()));
    List<ConsumerRecord<String, String>> consumerRecords =
        consumeAllRecordsFromTopic(
            "json-complex-topic", StringDeserializer.class, StringDeserializer.class);

    assertEquals(3, consumerRecords.size());
    List<JsonNode> deserializedRecords = new ArrayList<>();
    for (ConsumerRecord<String, String> record : consumerRecords) {
      deserializedRecords.add(objectMapper.readTree(record.value()));
    }
    List<String> ids =
        deserializedRecords.stream().map(r -> r.get("id").asText()).collect(Collectors.toList());
    List<Integer> intItems =
        deserializedRecords.stream()
            .map(r -> r.get("int_item").asInt())
            .collect(Collectors.toList());
    List<List<Integer>> arrayItems =
        deserializedRecords.stream()
            .map(
                r ->
                    ImmutableList.of(
                        r.get("array").get(0).asInt(),
                        r.get("array").get(1).asInt(),
                        r.get("array").get(2).asInt()))
            .collect(Collectors.toList());

    assertThat(ids, hasItem("A001"));
    assertThat(ids, hasItem("A002"));
    assertThat(ids, hasItem("A003"));
    assertThat(intItems, hasItem(9));
    assertThat(intItems, hasItem(0));
    assertThat(arrayItems.get(0), hasItem(1));
    assertThat(arrayItems.get(0), hasItem(2));
    assertThat(arrayItems.get(0), hasItem(3));
  }

  @Test
  public void testSimpleAvro() throws IOException {
    ConfigSource configSource = embulk.loadYamlResource("config_simple_avro.yml");
    setBootstrapServer(configSource);

    embulk.runOutput(
        configSource, Paths.get(Resources.getResource("org/embulk/test/in1.csv").getPath()));

    SchemaRegistryClient schemaRegistryClient =
        MockSchemaRegistry.getClientForScope("embulk-output-kafka");
    try (KafkaAvroDeserializer kafkaAvroDeserializer =
        new KafkaAvroDeserializer(schemaRegistryClient)) {

      List<ConsumerRecord<byte[], byte[]>> consumerRecords =
          consumeAllRecordsFromTopic(
              "avro-simple-topic", ByteArrayDeserializer.class, ByteArrayDeserializer.class);

      assertEquals(3, consumerRecords.size());
      List<GenericRecord> genericRecords =
          consumerRecords.stream()
              .map(
                  r ->
                      (GenericRecord)
                          kafkaAvroDeserializer.deserialize("avro-simple-topic", r.value()))
              .collect(Collectors.toList());

      List<String> ids =
          genericRecords.stream()
              .map(r -> String.valueOf(r.get("id")))
              .collect(Collectors.toList());
      List<Long> intItems =
          genericRecords.stream().map(r -> (Long) r.get("int_item")).collect(Collectors.toList());
      List<String> varcharItems =
          genericRecords.stream()
              .map(r -> String.valueOf(r.get("varchar_item")))
              .collect(Collectors.toList());

      assertThat(ids, hasItem("A001"));
      assertThat(ids, hasItem("A002"));
      assertThat(ids, hasItem("A003"));
      assertThat(intItems, hasItem(1L));
      assertThat(intItems, hasItem(2L));
      assertThat(intItems, hasItem(3L));
      assertThat(varcharItems, hasItem("a"));
      assertThat(varcharItems, hasItem("b"));
      assertThat(varcharItems, hasItem("c"));
    }
  }

  @Test
  public void testSimpleAvroSchemaFromRegistry() throws IOException, RestClientException {
    ConfigSource configSource = embulk.loadYamlResource("config_simple_avro.yml");
    Object avsc = configSource.get(Object.class, "avsc");
    String avscString = objectMapper.writeValueAsString(avsc);
    configSource.set("avsc", null);
    ParsedSchema parsedSchema = new AvroSchema(avscString);
    MockSchemaRegistry.getClientForScope("embulk-output-kafka")
        .register("avro-simple-topic-value", parsedSchema);
    setBootstrapServer(configSource);

    embulk.runOutput(
        configSource, Paths.get(Resources.getResource("org/embulk/test/in1.csv").getPath()));

    SchemaRegistryClient schemaRegistryClient =
        MockSchemaRegistry.getClientForScope("embulk-output-kafka");
    try (KafkaAvroDeserializer kafkaAvroDeserializer =
        new KafkaAvroDeserializer(schemaRegistryClient)) {

      List<ConsumerRecord<byte[], byte[]>> consumerRecords =
          consumeAllRecordsFromTopic(
              "avro-simple-topic", ByteArrayDeserializer.class, ByteArrayDeserializer.class);

      assertEquals(3, consumerRecords.size());
      List<GenericRecord> genericRecords =
          consumerRecords.stream()
              .map(
                  r ->
                      (GenericRecord)
                          kafkaAvroDeserializer.deserialize("avro-simple-topic", r.value()))
              .collect(Collectors.toList());

      List<String> ids =
          genericRecords.stream()
              .map(r -> String.valueOf(r.get("id")))
              .collect(Collectors.toList());
      List<Long> intItems =
          genericRecords.stream().map(r -> (Long) r.get("int_item")).collect(Collectors.toList());
      List<String> varcharItems =
          genericRecords.stream()
              .map(r -> String.valueOf(r.get("varchar_item")))
              .collect(Collectors.toList());

      assertThat(ids, hasItem("A001"));
      assertThat(ids, hasItem("A002"));
      assertThat(ids, hasItem("A003"));
      assertThat(intItems, hasItem(1L));
      assertThat(intItems, hasItem(2L));
      assertThat(intItems, hasItem(3L));
      assertThat(varcharItems, hasItem("a"));
      assertThat(varcharItems, hasItem("b"));
      assertThat(varcharItems, hasItem("c"));
    }
  }

  @Test
  public void testSimpleAvroAvscFile() throws IOException {
    ConfigSource configSource = embulk.loadYamlResource("config_simple_avro_avsc_file.yml");
    setBootstrapServer(configSource);

    embulk.runOutput(
        configSource, Paths.get(Resources.getResource("org/embulk/test/in1.csv").getPath()));

    SchemaRegistryClient schemaRegistryClient =
        MockSchemaRegistry.getClientForScope("embulk-output-kafka");
    try (KafkaAvroDeserializer kafkaAvroDeserializer =
        new KafkaAvroDeserializer(schemaRegistryClient)) {

      List<ConsumerRecord<byte[], byte[]>> consumerRecords =
          consumeAllRecordsFromTopic(
              "avro-simple-topic", ByteArrayDeserializer.class, ByteArrayDeserializer.class);

      assertEquals(3, consumerRecords.size());
      List<GenericRecord> genericRecords =
          consumerRecords.stream()
              .map(
                  r ->
                      (GenericRecord)
                          kafkaAvroDeserializer.deserialize("avro-simple-topic", r.value()))
              .collect(Collectors.toList());

      List<String> ids =
          genericRecords.stream()
              .map(r -> String.valueOf(r.get("id")))
              .collect(Collectors.toList());
      List<Long> intItems =
          genericRecords.stream().map(r -> (Long) r.get("int_item")).collect(Collectors.toList());
      List<String> varcharItems =
          genericRecords.stream()
              .map(r -> String.valueOf(r.get("varchar_item")))
              .collect(Collectors.toList());

      assertThat(ids, hasItem("A001"));
      assertThat(ids, hasItem("A002"));
      assertThat(ids, hasItem("A003"));
      assertThat(intItems, hasItem(1L));
      assertThat(intItems, hasItem(2L));
      assertThat(intItems, hasItem(3L));
      assertThat(varcharItems, hasItem("a"));
      assertThat(varcharItems, hasItem("b"));
      assertThat(varcharItems, hasItem("c"));
    }
  }

  @Test
  public void testSimpleAvroComplex() throws IOException {
    ConfigSource configSource = embulk.loadYamlResource("config_complex_avro.yml");
    setBootstrapServer(configSource);

    embulk.runOutput(
        configSource, Paths.get(Resources.getResource("org/embulk/test/in_complex.csv").getPath()));

    SchemaRegistryClient schemaRegistryClient =
        MockSchemaRegistry.getClientForScope("embulk-output-kafka");
    try (KafkaAvroDeserializer kafkaAvroDeserializer =
        new KafkaAvroDeserializer(schemaRegistryClient)) {

      List<ConsumerRecord<byte[], byte[]>> consumerRecords =
          consumeAllRecordsFromTopic(
              "avro-complex-topic", ByteArrayDeserializer.class, ByteArrayDeserializer.class);

      assertEquals(3, consumerRecords.size());
      List<GenericRecord> genericRecords =
          consumerRecords.stream()
              .map(
                  r ->
                      (GenericRecord)
                          kafkaAvroDeserializer.deserialize("avro-complex-topic", r.value()))
              .collect(Collectors.toList());

      List<String> ids =
          genericRecords.stream()
              .map(r -> String.valueOf(r.get("id")))
              .collect(Collectors.toList());
      List<Long> intItems =
          genericRecords.stream().map(r -> (Long) r.get("int_item")).collect(Collectors.toList());
      List<Instant> timeItems =
          genericRecords.stream()
              .map(r -> Instant.ofEpochMilli((long) r.get("time")))
              .collect(Collectors.toList());

      assertThat(ids, hasItem("A001"));
      assertThat(ids, hasItem("A002"));
      assertThat(ids, hasItem("A003"));
      assertThat(intItems, hasItem(9L));
      assertThat(intItems, hasItem(0L));
      assertThat(timeItems, hasItem(Instant.parse("2018-02-01T12:15:18.000Z")));
      assertThat(timeItems, hasItem(Instant.parse("2018-02-02T12:15:18.000Z")));
      assertThat(timeItems, hasItem(Instant.parse("2018-02-03T12:15:18.000Z")));
    }
  }

  @Test
  public void testKeyColumnConfig() throws IOException {
    ConfigSource configSource = embulk.loadYamlResource("config_with_key_column.yml");
    setBootstrapServer(configSource);
    embulk.runOutput(
        configSource, Paths.get(Resources.getResource("org/embulk/test/in1.csv").getPath()));
    List<ConsumerRecord<String, String>> consumerRecords =
        consumeAllRecordsFromTopic(
            "json-topic", StringDeserializer.class, StringDeserializer.class);

    assertEquals(3, consumerRecords.size());
    List<String> keys = new ArrayList<>();
    for (ConsumerRecord<String, String> record : consumerRecords) {
      keys.add(record.key());
    }

    assertThat(keys, hasItem("A001"));
    assertThat(keys, hasItem("A002"));
    assertThat(keys, hasItem("A003"));
  }

  @Test
  public void testPartitionColumnConfig() throws IOException {
    ConfigSource configSource = embulk.loadYamlResource("config_with_partition_column.yml");
    setBootstrapServer(configSource);
    embulk.runOutput(
        configSource, Paths.get(Resources.getResource("org/embulk/test/in1.csv").getPath()));
    List<ConsumerRecord<String, String>> consumerRecords =
        consumeAllRecordsFromTopic(
            "json-topic", StringDeserializer.class, StringDeserializer.class);

    assertEquals(3, consumerRecords.size());
    List<Integer> partitions = new ArrayList<>();
    for (ConsumerRecord<String, String> record : consumerRecords) {
      partitions.add(record.partition());
    }

    assertThat(partitions, hasItem(1));
    assertThat(partitions, hasItem(2));
    assertThat(partitions, hasItem(3));
  }

  @Test
  public void testColumnForDeletion() throws IOException {
    ConfigSource configSource = embulk.loadYamlResource("config_with_column_for_deletion.yml");
    setBootstrapServer(configSource);
    embulk.runOutput(
        configSource,
        Paths.get(Resources.getResource("org/embulk/test/in_with_deletion.csv").getPath()));
    List<ConsumerRecord<String, String>> consumerRecords =
        consumeAllRecordsFromTopic(
            "json-topic", StringDeserializer.class, StringDeserializer.class);

    assertEquals(3, consumerRecords.size());
    HashMap<String, String> recordMap = new HashMap<>();
    consumerRecords.forEach(record -> recordMap.put(record.key(), record.value()));
    assertNotNull(recordMap.get("A001"));
    assertNotNull(recordMap.get("A003"));
    assertNull(recordMap.get("A002"));
  }

  @Test
  public void testColumnForDeletionAvro() throws IOException {
    ConfigSource configSource = embulk.loadYamlResource("config_with_column_for_deletion_avro.yml");
    setBootstrapServer(configSource);
    embulk.runOutput(
        configSource,
        Paths.get(Resources.getResource("org/embulk/test/in_with_deletion.csv").getPath()));
    List<ConsumerRecord<String, String>> consumerRecords =
        consumeAllRecordsFromTopic(
            "avro-simple-topic", StringDeserializer.class, StringDeserializer.class);

    assertEquals(3, consumerRecords.size());
    HashMap<String, String> recordMap = new HashMap<>();
    consumerRecords.forEach(record -> recordMap.put(record.key(), record.value()));
    assertNotNull(recordMap.get("A001"));
    assertNotNull(recordMap.get("A003"));
    assertNull(recordMap.get("A002"));
  }
}
