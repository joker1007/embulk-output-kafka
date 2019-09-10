package org.embulk.output.kafka;

import com.google.common.io.Resources;
import org.embulk.config.ConfigSource;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.test.TestingEmbulk;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import static org.junit.Assert.assertTrue;

@SuppressWarnings("UnstableApiUsage")
public class TestKafkaOutputPlugin
{
    @Rule
    public TestingEmbulk embulk = TestingEmbulk.builder()
            .registerPlugin(OutputPlugin.class, "kafka", KafkaOutputPlugin.class)
            .build();

    @Before
    public void setup()
    {
        System.out.println("test");
    }

    @Test
    public void testConfigLoad() throws IOException {
        ConfigSource config = embulk.loadYamlResource("config_simple.yml");
        String resourcePath = Resources.getResource("in1.csv").getPath();
        Path in = FileSystems.getDefault().getPath(resourcePath);

        embulk.runOutput(config, in);
    }
}
