package io.goobi.api.job;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;
import org.goobi.production.flow.jobs.AbstractGoobiJob;
import org.goobi.vocabulary.Definition;
import org.goobi.vocabulary.VocabRecord;
import org.goobi.vocabulary.Vocabulary;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

import de.sub.goobi.config.ConfigPlugins;
import de.sub.goobi.persistence.managers.VocabularyManager;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class HerisQuartzPlugin extends AbstractGoobiJob {

    private XMLConfiguration config;

    @Getter
    private String herisFolder;

    @Getter
    private long lastRunMillis;

    // sftp access
    @Getter
    private String knownHosts;
    @Getter
    private String username;
    @Getter
    private String hostname;
    @Getter
    private String password;
    @Getter
    private String ftpFolder;

    @Getter
    @Setter
    private Path jsonFile;

    private String vocabularyName;

    private Vocabulary vocabulary;

    @Override
    public void execute() {

        parseConfiguration();

        // search for latest heris file

        // check, if it is newer than the last import

        // parse file

        // convert into vocabulary records

        // compare to existing records
        // - update changed data
        // - add new data

        // save records
    }

    @Override
    public String getJobName() {
        return "intranda_quartz_herisJob";
    }

    public void parseConfiguration() {
        config = ConfigPlugins.getPluginConfig(getJobName());
        config.setExpressionEngine(new XPathExpressionEngine());
        config.setReloadingStrategy(new FileChangedReloadingStrategy());
        herisFolder = config.getString("/herisFolder");
        lastRunMillis = config.getLong("/lastrun", 0l);

        username = config.getString("/sftp/username");
        password = config.getString("/sftp/password");
        hostname = config.getString("/sftp/hostname");
        knownHosts = config.getString("/sftp/knownHosts", System.getProperty("user.home").concat("/.ssh/known_hosts"));
        ftpFolder = config.getString("/sftp/sftpFolder");

        vocabularyName = config.getString("/vocabulary/@name");

    }

    private Path getLatestHerisFile() {
        String jsonFile = "";
        try {
            // open sftp connection
            ChannelSftp sftpChannel = openSftpConnection();
            sftpChannel.connect();

            // list files in configured directory
            List<LsEntry> lsList = sftpChannel.ls(ftpFolder);
            for (LsEntry lsEntry : lsList) {
                // TODO find newest .json file
                jsonFile = lsEntry.getFilename();
                lsEntry.getAttrs().getATime();
            }
            // TODO download file into temp folder
            Path destination = Paths.get(herisFolder, jsonFile);
            sftpChannel.get(jsonFile, destination.toString());

            // close connection
            sftpChannel.disconnect();

            return destination;
        } catch (JSchException | SftpException e) {
            log.error(e);
        }
        return null;
    }

    /**
     * 
     * @return ChannelSftp object
     * @throws JSchException
     */
    private ChannelSftp openSftpConnection() throws JSchException {
        JSch jsch = new JSch();
        jsch.setKnownHosts(knownHosts);
        Session jschSession = jsch.getSession(username, hostname);
        jschSession.setPassword(password);
        jschSession.connect();
        return (ChannelSftp) jschSession.openChannel("sftp");
    }

    private void updateLastRun() {
        try {
            config.setProperty("lastRun", lastRunMillis);
            Path configurationFile = Paths.get(config.getBasePath(), "plugin_" + getJobName() + ".xml");
            this.config.save(configurationFile.toString());
        } catch (ConfigurationException e) {
            log.error("Error while updating the configuration file", e);
        }
    }

    public void generateRecordsFromFile() {
        vocabulary = VocabularyManager.getVocabularyByTitle(vocabularyName);
        VocabularyManager.getAllRecords(vocabulary);
        List<VocabRecord> existingRecords = vocabulary.getRecords();

        List<Definition> structure = vocabulary.getStruct();

    }
}
