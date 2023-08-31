package io.goobi.api.job;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;
import org.apache.commons.lang.StringUtils;
import org.goobi.production.flow.jobs.AbstractGoobiJob;
import org.goobi.vocabulary.Definition;
import org.goobi.vocabulary.Field;
import org.goobi.vocabulary.VocabRecord;
import org.goobi.vocabulary.Vocabulary;

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
import de.sub.goobi.persistence.managers.VocabularyManager;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class HerisQuartzPlugin extends AbstractGoobiJob {

    // folder where the gets stored temporary
    @Getter
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

    // mapping between json element and vocabulary field
    @Getter
    private Map<String, String> jsonMapping;

    @Getter
    private Vocabulary vocabulary;

    // identifier fields in vocabulary and json file
    private String identifierVocabField;
    private String identifierJsonField;

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
        generateRecordsFromFile();

        // save records
        VocabularyManager.saveRecords(vocabulary);

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

        vocabulary = VocabularyManager.getVocabularyByTitle(vocabularyName);
        VocabularyManager.getAllRecords(vocabulary);
    }

    /**
     * Download the latest json file from configured sftp server
     * 
     * @return path to downloaded file or null
     */

    public Path getLatestHerisFile() {
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
        return null;
    }

    /**
     * Convert json file into VocabRecord
     */
    public void generateRecordsFromFile() {

        // open json file
        try (InputStream is = new FileInputStream(jsonFile.toFile())) {
            Object document = Configuration.defaultConfiguration().jsonProvider().parse(is, StandardCharsets.UTF_8.toString());
            // split array into single objects
            Object object = JsonPath.read(document, "$.*");
            if (object instanceof List) {
                List<?> records = (List<?>) object;

                // parse metadata
                for (Object jsonRecord : records) {
                    parseRecord(jsonRecord);
                }
            }
        } catch (IOException e) {
            log.error(e);
        }

    }

    /*
     * Convert a single json object, update the vocabulary record
     */
    private void parseRecord(Object jsonRecord) {
        VocabRecord vocabRecord = null;

        // get identifier field
        String identifierValue = (String) JsonPath.read(jsonRecord, identifierJsonField);

        vocabRecord = getRecord(identifierValue);

        // add values to fields

        for (Entry<String, String> entry : jsonMapping.entrySet()) {
            String fieldName = entry.getKey();
            String jsonPath = entry.getValue();

            Field field = vocabRecord.getFieldByLabel(fieldName);
            if (field != null) {
                Object val = JsonPath.read(jsonRecord, jsonPath);
                if (val != null) {
                    String value = (String) val;
                    field.setValue(value);
                } else {
                    field.setValue("");
                }
            }
        }
    }

    /*
     * Find an existing record for the given identifier or create a new record
     * 
     */

    private VocabRecord getRecord(String identifierValue) {
        // check existing records, if identifier exists
        VocabRecord vocabRecord = null;
        for (VocabRecord vr : vocabulary.getRecords()) {
            String recId = vr.getFieldByLabel(identifierVocabField).getValue();
            if (identifierValue.equals(recId)) {
                log.debug("found existing record for id {}", identifierValue);
                vocabRecord = vr;
                break;
            }
        }

        // otherwise create a new record
        if (vocabRecord == null) {
            log.debug("create new record for id {}", identifierValue);
            vocabRecord = new VocabRecord();
            vocabRecord.setVocabularyId(vocabulary.getId());
            vocabulary.getRecords().add(vocabRecord);

            List<Field> fieldList = new ArrayList<>();
            for (Definition definition : vocabulary.getStruct()) {
                Field field = new Field(definition.getLabel(), definition.getLanguage(), "", definition);
                fieldList.add(field);
            }
            vocabRecord.setFields(fieldList);
        }
        return vocabRecord;
    }
}
