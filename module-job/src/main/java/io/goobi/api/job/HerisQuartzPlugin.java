package io.goobi.api.job;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import de.sub.goobi.config.ConfigPlugins;
import de.sub.goobi.helper.StorageProvider;
import io.goobi.vocabulary.exchange.FieldDefinition;
import io.goobi.vocabulary.exchange.FieldInstance;
import io.goobi.vocabulary.exchange.FieldValue;
import io.goobi.vocabulary.exchange.TranslationInstance;
import io.goobi.vocabulary.exchange.Vocabulary;
import io.goobi.vocabulary.exchange.VocabularyRecord;
import io.goobi.vocabulary.exchange.VocabularySchema;
import io.goobi.workflow.api.vocabulary.VocabularyAPIManager;
import io.goobi.workflow.api.vocabulary.VocabularyRecordAPI;
import io.goobi.workflow.api.vocabulary.helper.ExtendedVocabularyRecord;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;
import org.apache.commons.lang.StringUtils;
import org.goobi.production.flow.jobs.AbstractGoobiJob;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

@Log4j2
public class HerisQuartzPlugin extends AbstractGoobiJob {

    // folder where the gets stored temporary
    @Getter
    @Setter
    private String herisFolder;

    // sftp access
    @Getter
    @Setter
    private String username;
    @Getter
    @Setter
    private String password;
    @Getter
    @Setter
    private String keyfile;
    @Getter
    private String hostname;
    @Getter
    private String knownHosts;
    @Getter
    private String ftpFolder;
    @Getter
    private String pubkeyAcceptedAlgorithms;

    @Getter
    private int port = 22;

    // downloaded json file
    @Getter
    @Setter
    private Path jsonFile;

    // name of the vocabulary to enrich
    @Getter
    private String vocabularyName;

    @Getter
    private Vocabulary vocabulary;

    @Getter
    private List<VocabularyRecord> parsedRecords = Collections.emptyList();

    private VocabularySchema vocabularySchema;

    private long vocabularyId;

    private VocabularyAPIManager vocabularyAPI = VocabularyAPIManager.getInstance();

    // mapping between json element and vocabulary field
    @Getter
    private Map<String, String> jsonMapping;


    // identifier fields in vocabulary and json file
    private String identifierVocabField;
    private long identifierVocabFieldId;
    private String identifierJsonField;

    @Setter
    private boolean useSFTP;

    /**
     * When called, this method gets executed
     * 
     * It will - download the latest json file from the configured sftp server - convert it into vocabulary records - save the new records
     * 
     */

    @Override
    public void execute() {

        parseConfiguration();
        // search for latest json file
        jsonFile = getLatestHerisFile();

        if (jsonFile == null) {
            log.info("No import file found, continue");
            return;
        }

        // file parsing and conversion into vocabulary records
        parsedRecords = generateRecordsFromFile();

        // save records
        VocabularyRecordAPI recordAPI = vocabularyAPI.vocabularyRecords();
        parsedRecords.forEach(recordAPI::save);

        // delete downloaded file
        try {
            StorageProvider.getInstance().deleteFile(jsonFile);
        } catch (IOException e) {
            log.error(e);
        }
    }

    @Override
    public String getJobName() {
        return "intranda_quartz_herisJob";
    }

    /**
     * Parse the configuration file
     * 
     */

    public void parseConfiguration() {
        jsonMapping = new HashMap<>();

        XMLConfiguration config = ConfigPlugins.getPluginConfig(getJobName());
        config.setExpressionEngine(new XPathExpressionEngine());
        config.setReloadingStrategy(new FileChangedReloadingStrategy());
        herisFolder = config.getString("/herisFolder");

        useSFTP = config.getBoolean("/sftp/@use", true);
        username = config.getString("/sftp/username");
        password = config.getString("/sftp/password");
        hostname = config.getString("/sftp/hostname");
        port = config.getInt("/sftp/port", 22);
        keyfile = config.getString("/sftp/keyfile");
        knownHosts = config.getString("/sftp/knownHosts", System.getProperty("user.home").concat("/.ssh/known_hosts"));
        ftpFolder = config.getString("/sftp/sftpFolder");
        pubkeyAcceptedAlgorithms = config.getString("/sftp/pubkeyAcceptedAlgorithms");
        vocabularyName = config.getString("/vocabulary/@name");

        List<HierarchicalConfiguration> fields = config.configurationsAt("/vocabulary/field");
        for (HierarchicalConfiguration hc : fields) {
            jsonMapping.put(hc.getString("@fieldName"), hc.getString("@jsonPath"));
            if (hc.getBoolean("@identifier", false)) {
                identifierVocabField = hc.getString("@fieldName");
                identifierJsonField = hc.getString("@jsonPath");
            }
        }

        vocabulary = vocabularyAPI.vocabularies().findByName(vocabularyName);
        vocabularyId = vocabulary.getId();
        vocabularySchema = vocabularyAPI.vocabularySchemas().get(vocabulary.getSchemaId());
        identifierVocabFieldId = vocabularySchema.getDefinitions().stream()
                .filter(d -> d.getName().equals(identifierVocabField))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Field \"" + identifierVocabField + "\" does not exist in vocabulary \"" + vocabularyName + "\""))
                .getId();
    }

    /**
     * Download the latest json file from configured sftp server
     * 
     * @return path to downloaded file or null
     */

    public Path getLatestHerisFile() {
        if (useSFTP) {
            try {
                // open sftp connection
                JSch jsch = new JSch();
                if (StringUtils.isNotBlank(keyfile) && StringUtils.isNotBlank(password)) {
                    jsch.addIdentity(keyfile, password);
                } else if (StringUtils.isNotBlank(keyfile)) {
                    jsch.addIdentity(keyfile);
                }
                //            jschSession.setPort(443);// NOSONAR use this, if other port than 22 is needed
                jsch.setKnownHosts(knownHosts);
                Session jschSession = jsch.getSession(username, hostname, port);
                if (StringUtils.isBlank(keyfile)) {
                    jschSession.setPassword(password);
                }
                if (StringUtils.isNotBlank(pubkeyAcceptedAlgorithms)) {
                    Properties config = new Properties();
                    config.put("PubkeyAcceptedAlgorithms", pubkeyAcceptedAlgorithms);
                    jschSession.setConfig(config);
                }
                jschSession.connect();
                ChannelSftp sftpChannel = (ChannelSftp) jschSession.openChannel("sftp");
                sftpChannel.connect();
                // list files in configured directory
                List<LsEntry> lsList = sftpChannel.ls(ftpFolder);
                int timestamp = 0;
                String filename = null;
                for (LsEntry lsEntry : lsList) {

                    if (lsEntry.getFilename().endsWith(".json") && (timestamp == 0 || timestamp < lsEntry.getAttrs().getMTime())) {
                        timestamp = lsEntry.getAttrs().getMTime();
                        filename = lsEntry.getFilename();
                    }
                }
                Path destination = Paths.get(herisFolder, filename);
                sftpChannel.get(ftpFolder + filename, destination.toString());
                // close connection
                sftpChannel.disconnect();
                jschSession.disconnect();
                return destination;
            } catch (JSchException | SftpException e) {
                log.error(e);
            }
        } else {
            try (Stream<Path> walk = Files.walk(Path.of(herisFolder))) {
                return walk.filter(p -> !Files.isDirectory(p))
                        .filter(p -> p.toString().toLowerCase().endsWith(".json"))
                        .max(Comparator.comparing(p -> p.toFile().lastModified(), Long::compare))
                        .orElse(null);
            } catch (IOException e) {
                log.error(e);
            }
        }
        return null;
    }

    /**
     * Convert json file into VocabRecord
     */
    public List<VocabularyRecord> generateRecordsFromFile() {
        List<VocabularyRecord> result = new LinkedList<>();

        // open json file
        try (InputStream is = new FileInputStream(jsonFile.toFile())) {
            Object document = Configuration.defaultConfiguration().jsonProvider().parse(is, StandardCharsets.UTF_8.toString());
            // split array into single objects
            Object object = JsonPath.read(document, "$.*");
            if (object instanceof List) {
                List<?> records = (List<?>) object;

                // parse metadata
                records.forEach(r -> result.add(parseRecord(r)));
            }
        } catch (IOException e) {
            log.error(e);
        }

        return result;
    }

    /*
     * Convert a single json object, update the vocabulary record
     */
    private VocabularyRecord parseRecord(Object jsonRecord) {
        // get identifier field
        String identifierValue = (String) JsonPath.read(jsonRecord, identifierJsonField);

        VocabularyRecord vocabRecord = findOrCreateNewRecord(identifierValue);

        // add or overwrite values

        for (Map.Entry<String, String> entry : jsonMapping.entrySet()) {
            String fieldName = entry.getKey();
            String jsonPath = entry.getValue();

            Optional<Long> definitionId = vocabularySchema.getDefinitions().stream()
                    .filter(d -> d.getName().equals(fieldName))
                    .findFirst()
                    .map(FieldDefinition::getId);

            if (definitionId.isEmpty()) {
                continue;
            }

            // remove existing field
            vocabRecord.getFields().removeIf(f -> f.getDefinitionId().equals(definitionId.get()));

            Object val = JsonPath.read(jsonRecord, jsonPath);
            if (val == null) {
                continue;
            }

            FieldInstance field = new FieldInstance();
            field.setDefinitionId(definitionId.get());
            field.setRecordId(vocabRecord.getId()); // This is either null for new records or existing id for existing records (API expects this like this)
            TranslationInstance translationInstance = new TranslationInstance();
            translationInstance.setValue((String) val);
            FieldValue fieldValue = new FieldValue();
            fieldValue.setTranslations(List.of(translationInstance));
            field.setValues(List.of(fieldValue));

            vocabRecord.getFields().add(field);
        }

        return vocabRecord;
    }

    /*
     * Find an existing record for the given identifier or create a new record
     * 
     */
    private ExtendedVocabularyRecord findOrCreateNewRecord(String identifierValue) {
        List<ExtendedVocabularyRecord> results = vocabularyAPI.vocabularyRecords()
                .list(vocabularyId)
                .search(identifierVocabFieldId + ":" + identifierValue)
                .request()
                .getContent();

        if (results.isEmpty()) {
            return vocabularyAPI.vocabularyRecords().createEmptyRecord(vocabularyId, null, false);
        } else if (results.size() == 1) {
            return results.get(0);
        } else {
            throw new IllegalArgumentException("Found vocabulary record not unique, skipping import of this entry");
        }
    }
}
