package io.goobi.api.job;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.BeforeClass;
import org.junit.Test;

import io.goobi.api.job.HerisQuartzPlugin;

//@RunWith(PowerMockRunner.class)
//@PowerMockIgnore({ "javax.management.*", "javax.net.ssl.*" ,"jdk.internal.reflect.*"})
public class HerisQuartzPluginTest {

    private static String resourcesFolder;

    @BeforeClass
    public static void setUpClass() throws Exception {
        resourcesFolder = "src/test/resources/"; // for junit tests in eclipse

        if (!Files.exists(Paths.get(resourcesFolder))) {
            resourcesFolder = "target/test-classes/"; // to run mvn test from cli or in jenkins
        }

        String log4jFile = resourcesFolder + "log4j2.xml"; // for junit tests in eclipse

        System.setProperty("log4j.configurationFile", log4jFile);
    }

    @Test
    public void testTitle() throws IOException {
        HerisQuartzPlugin job = new HerisQuartzPlugin();
        assertEquals("HerisJob", job.getJobName());

    }
}
