Embulk::JavaPlugin.register_output(
  "kafka", "org.embulk.output.kafka.KafkaOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
