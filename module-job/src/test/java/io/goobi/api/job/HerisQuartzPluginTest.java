package io.goobi.api.job;

import de.sub.goobi.config.ConfigurationHelper;
import de.sub.goobi.helper.Helper;
import io.goobi.vocabulary.exchange.FieldDefinition;
import io.goobi.vocabulary.exchange.Vocabulary;
import io.goobi.vocabulary.exchange.VocabularyRecord;
import io.goobi.vocabulary.exchange.VocabularySchema;
import io.goobi.workflow.api.vocabulary.VocabularyAPI;
import io.goobi.workflow.api.vocabulary.VocabularyAPIManager;
import io.goobi.workflow.api.vocabulary.VocabularyRecordAPI;
import io.goobi.workflow.api.vocabulary.VocabularySchemaAPI;
import io.goobi.workflow.api.vocabulary.hateoas.VocabularyRecordPageResult;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ ConfigurationHelper.class, VocabularyAPIManager.class, Helper.class })
@PowerMockIgnore({ "javax.management.*", "javax.net.ssl.*", "jdk.internal.reflect.*", "com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "org.w3c.*", "javax.crypto.*", "javax.crypto.JceSecurity" })
public class HerisQuartzPluginTest {

    private static String resourcesFolder;

    private static Path jsonFile;

    private VocabularySchema vocabularySchema;

    private long idCounter;

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
        idCounter = 0;

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

        VocabularySchemaAPI vocabularySchemaAPI = EasyMock.createMock(VocabularySchemaAPI.class);
        vocabularySchema = new VocabularySchema();
        vocabularySchema.setId(idCounter++);
        vocabularySchema.setDefinitions(prepareSchemaDefinitions());
        EasyMock.expect(vocabularySchemaAPI.get(vocabularySchema.getId())).andReturn(vocabularySchema).anyTimes();
        EasyMock.replay(vocabularySchemaAPI);

        VocabularyAPI vocabularyAPI = EasyMock.createMock(VocabularyAPI.class);
        Vocabulary vocabulary = new Vocabulary();
        vocabulary.setName("HERIS");
        vocabulary.setId(idCounter++);
        vocabulary.setSchemaId(vocabularySchema.getId());
        EasyMock.expect(vocabularyAPI.findByName("HERIS")).andReturn(vocabulary).anyTimes();
        EasyMock.replay(vocabularyAPI);

        VocabularyRecordAPI vocabularyRecordAPI = EasyMock.createMock(VocabularyRecordAPI.class);
        // As merging was not tested before, we assume all records not existing
        VocabularyRecordPageResult emptyResult = new VocabularyRecordPageResult();
        EasyMock.expect(vocabularyRecordAPI.search(EasyMock.anyLong(), EasyMock.anyString())).andReturn(emptyResult).anyTimes();
        EasyMock.replay(vocabularyRecordAPI);

        PowerMock.mockStatic(VocabularyAPIManager.class);
        VocabularyAPIManager vocabularyAPIManager = EasyMock.createMock(VocabularyAPIManager.class);
        EasyMock.expect(VocabularyAPIManager.getInstance()).andReturn(vocabularyAPIManager).anyTimes();
        EasyMock.expect(vocabularyAPIManager.vocabularies()).andReturn(vocabularyAPI).anyTimes();
        EasyMock.expect(vocabularyAPIManager.vocabularySchemas()).andReturn(vocabularySchemaAPI).anyTimes();
        EasyMock.expect(vocabularyAPIManager.vocabularyRecords()).andReturn(vocabularyRecordAPI).anyTimes();
        PowerMock.replay(VocabularyAPIManager.class);
        EasyMock.replay(vocabularyAPIManager);
    }

    private List<FieldDefinition> prepareSchemaDefinitions() {
        List<FieldDefinition> result = new LinkedList<>();
        result.add(createFieldDefinition("herisid", true, true));
        result.add(createFieldDefinition("objektid", false, true));
        result.add(createFieldDefinition("title", false, false));
        result.add(createFieldDefinition("type", false, false));
        result.add(createFieldDefinition("mainCategoryA", false, false));
        result.add(createFieldDefinition("mainCategoryB", false, false));
        result.add(createFieldDefinition("mainCategoryC", false, false));
        result.add(createFieldDefinition("subCategory", false, false));
        return result;
    }

    private FieldDefinition createFieldDefinition(String name, boolean mainValue, boolean unique) {
        FieldDefinition result = new FieldDefinition();
        result.setId(idCounter++);
        result.setSchemaId(vocabularySchema.getId());
        result.setName(name);
        result.setMainEntry(mainValue);
        result.setTitleField(mainValue);
        result.setUnique(unique);
        return result;
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
    public void testSftpUsageDeactivated() {

        HerisQuartzPlugin plugin = new HerisQuartzPlugin();
        plugin.parseConfiguration();
        String username = System.getenv("SFTP_USERNAME");
        String password = System.getenv("SFTP_PASSWORD");
        plugin.setUseSFTP(false);
        plugin.setHerisFolder(resourcesFolder);
        plugin.setUsername(username);
        plugin.setPassword(password);

        Path expected = Paths.get(resourcesFolder, "sample_latest.json");
        expected.toFile()
                .setLastModified(Instant.now().toEpochMilli());

        Path actual = plugin.getLatestHerisFile();
        assertEquals(expected, actual);
    }

    @Test
    public void testReadJsonFile() throws Exception {
        HerisQuartzPlugin plugin = new HerisQuartzPlugin();
        plugin.parseConfiguration();

        plugin.setJsonFile(jsonFile);
        assertTrue(Files.exists(plugin.getJsonFile()));

        List<VocabularyRecord> parsedRecords = plugin.generateRecordsFromFile();

        assertEquals(87, parsedRecords.size());
    }
}
