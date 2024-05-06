package io.goobi.api.job;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.easymock.EasyMock;
import org.goobi.vocabulary.Definition;
import org.goobi.vocabulary.Field;
import org.goobi.vocabulary.VocabRecord;
import org.goobi.vocabulary.Vocabulary;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import de.sub.goobi.config.ConfigurationHelper;
import de.sub.goobi.helper.Helper;
import de.sub.goobi.persistence.managers.VocabularyManager;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ ConfigurationHelper.class, VocabularyManager.class, Helper.class })
@PowerMockIgnore({ "javax.management.*", "javax.net.ssl.*", "jdk.internal.reflect.*", "com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
    "org.w3c.*", "javax.crypto.*", "javax.crypto.JceSecurity" })
public class HerisQuartzPluginTest {

    private static String resourcesFolder;

    private static Path jsonFile;

    @BeforeClass
    public static void setUpClass() throws Exception {
        resourcesFolder = "src/test/resources/"; // for junit tests in eclipse

        if (!Files.exists(Paths.get(resourcesFolder))) {
            resourcesFolder = "target/test-classes/"; // to run mvn test from cli or in jenkins
        }

        String log4jFile = resourcesFolder + "log4j2.xml"; // for junit tests in eclipse

        System.setProperty("log4j.configurationFile", log4jFile);

        jsonFile = Paths.get(resourcesFolder, "sample.json");

    }

    @Before
    public void setUp() throws Exception {

        PowerMock.mockStatic(ConfigurationHelper.class);
        ConfigurationHelper configurationHelper = EasyMock.createMock(ConfigurationHelper.class);
        EasyMock.expect(ConfigurationHelper.getInstance()).andReturn(configurationHelper).anyTimes();
        EasyMock.expect(configurationHelper.getConfigurationFolder()).andReturn(resourcesFolder).anyTimes();
        EasyMock.expect(configurationHelper.useS3()).andReturn(false).anyTimes();

        EasyMock.replay(configurationHelper);
        PowerMock.replay(ConfigurationHelper.class);

        PowerMock.mockStatic(Helper.class);
        EasyMock.expect(Helper.getCurrentUser()).andReturn(null).anyTimes();
        PowerMock.replay(Helper.class);

        PowerMock.mockStatic(VocabularyManager.class);

        Vocabulary vocabulary = prepareVocabulary();

        EasyMock.expect(VocabularyManager.getVocabularyByTitle(EasyMock.anyString())).andReturn(vocabulary).anyTimes();
        VocabularyManager.getAllRecords(EasyMock.anyObject(Vocabulary.class));

        PowerMock.replay(VocabularyManager.class);
    }

    @Test
    public void testTitle() throws Exception {
        HerisQuartzPlugin job = new HerisQuartzPlugin();
        assertEquals("intranda_quartz_herisJob", job.getJobName());
    }

    @Test
    public void testConfiguration() throws Exception {
        HerisQuartzPlugin plugin = new HerisQuartzPlugin();

        plugin.parseConfiguration();
        assertEquals("/tmp/download", plugin.getHerisFolder());
        assertEquals("username", plugin.getUsername());
        assertEquals("password", plugin.getPassword());
        assertEquals("localhost", plugin.getHostname());
        assertEquals("~/.ssh/known_hosts", plugin.getKnownHosts());
        assertEquals("/tmp/", plugin.getFtpFolder());

        assertEquals("HERIS", plugin.getVocabularyName());
        assertEquals(9, plugin.getJsonMapping().size());
    }

    // enable this only if sftp server is installed and configured.
    // set environment variables SFTP_USERNAME and SFTP_PASSWORD
    @Test
    public void testSftpDownload() {

        HerisQuartzPlugin plugin = new HerisQuartzPlugin();
        plugin.parseConfiguration();
        String username = System.getenv("SFTP_USERNAME");
        String password = System.getenv("SFTP_PASSWORD");
        plugin.setUsername(username);
        plugin.setPassword(password);

        plugin.getLatestHerisFile();
    }

    @Test
    public void testReadJsonFile() throws Exception {
        HerisQuartzPlugin plugin = new HerisQuartzPlugin();
        plugin.parseConfiguration();

        plugin.setJsonFile(jsonFile);
        assertTrue(Files.exists(plugin.getJsonFile()));

        assertEquals(1, plugin.getVocabulary().getRecords().size());

        plugin.generateRecordsFromFile();

        assertEquals(87, plugin.getVocabulary().getRecords().size());
    }

    private Vocabulary prepareVocabulary() {
        Vocabulary vocabulary = new Vocabulary();
        vocabulary.setTitle("fixture");
        vocabulary.setDescription("fixture");

        // add definitions
        Definition herisIdDef = new Definition("herisid", "ger", "input", "", true, false, true, true);
        Definition objectIdDef = new Definition("objektid", "ger", "input", "", true, false, false, false);
        Definition titleDef = new Definition("title", "ger", "input", "", true, true, false, false);
        Definition typeDef = new Definition("type", "ger", "input", "", false, false, false, false);
        Definition mainCategoryDefA = new Definition("mainCategoryA", "ger", "input", "", false, false, false, false);
        Definition mainCategoryDefB = new Definition("mainCategoryB", "ger", "input", "", false, false, false, false);
        Definition mainCategoryDefC = new Definition("mainCategoryC", "ger", "input", "", false, false, false, false);
        Definition subCategoryDefA = new Definition("subCategory", "ger", "input", "", false, false, false, false);

        List<Definition> definitionList = new ArrayList<>();
        definitionList.add(herisIdDef);
        definitionList.add(objectIdDef);
        definitionList.add(titleDef);
        definitionList.add(typeDef);
        definitionList.add(mainCategoryDefA);
        definitionList.add(mainCategoryDefB);
        definitionList.add(mainCategoryDefC);
        definitionList.add(subCategoryDefA);
        vocabulary.setStruct(definitionList);

        // add sample record to test merging
        List<Field> fieldList = new ArrayList<>();

        //        "HERIS-ID": "112518",
        Field herisId = new Field();
        herisId.setLabel(herisIdDef.getLabel());
        herisId.setDefinition(herisIdDef);
        herisId.setValue("112518");
        fieldList.add(herisId);

        //        "Alte Objekt-ID": "130724",
        Field objectId = new Field();
        objectId.setLabel(objectIdDef.getLabel());
        objectId.setDefinition(objectIdDef);
        objectId.setValue("130724");
        fieldList.add(objectId);

        //        "Katalogtitel": "10 Fahrzeuge der Wiener Lokalbahn",
        Field title = new Field();
        title.setLabel(titleDef.getLabel());
        title.setDefinition(titleDef);
        title.setValue("10 Fahrzeuge der Wiener Lokalbahn");
        fieldList.add(title);

        //        "Typ": "Baudenkmal",
        Field type = new Field();
        type.setLabel(typeDef.getLabel());
        type.setDefinition(typeDef);
        type.setValue("Baudenkmal");
        fieldList.add(type);

        //        "Hauptkategorie grob": "Zubehör (bewegl/unbewegl.)",
        Field catA = new Field();
        catA.setLabel(mainCategoryDefA.getLabel());
        catA.setDefinition(mainCategoryDefA);
        catA.setValue("Zubehör (bewegl/unbewegl.)");
        fieldList.add(catA);

        //        "Hauptkategorie mittel": "sonstiges mobiles Zubehör",
        Field catB = new Field();
        catB.setLabel(mainCategoryDefB.getLabel());
        catB.setDefinition(mainCategoryDefB);
        catB.setValue("sonstiges mobiles Zubehör");
        fieldList.add(catB);

        //        "Hauptkategorie fein": "Fahrzeug",
        Field catC = new Field();
        catC.setLabel(mainCategoryDefC.getLabel());
        catC.setDefinition(mainCategoryDefC);
        catC.setValue("Fahrzeug");
        fieldList.add(catC);

        //        "Nebenkategorie grob": null,
        Field sub = new Field();
        sub.setLabel(subCategoryDefA.getLabel());
        sub.setDefinition(subCategoryDefA);
        sub.setValue(""); // null value is not allowed
        fieldList.add(sub);

        VocabRecord rec = new VocabRecord(1, 1, fieldList);
        rec.setId(1);
        vocabulary.getRecords().add(rec);

        return vocabulary;
    }
}
