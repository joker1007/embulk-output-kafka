package org.embulk.output.kafka;

import com.google.common.io.Resources;
import org.embulk.config.ConfigSource;
import org.embulk.spi.OutputPlugin;
import org.embulk.test.TestingEmbulk;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;

public class TestKafkaOutputPlugin
{
    @Rule
    public TestingEmbulk embulk = TestingEmbulk.builder()
            .registerPlugin(OutputPlugin.class, "kafka", KafkaOutputPlugin.class)
            .build();

    @Test
    public void testSimpleJson() throws IOException
    {
        ConfigSource configSource = embulk.loadYamlResource("config_simple.yml");
        embulk.runOutput(configSource, Paths.get(Resources.getResource("in1.csv").getPath()));
    }
}
