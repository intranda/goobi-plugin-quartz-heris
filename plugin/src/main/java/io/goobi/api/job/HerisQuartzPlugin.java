package io.goobi.api.job;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;
import org.goobi.production.flow.jobs.AbstractGoobiJob;

import de.sub.goobi.config.ConfigPlugins;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class HerisQuartzPlugin extends AbstractGoobiJob {

    private XMLConfiguration config;

    @Getter
    // TODO replace it with sftp call
    private String herisFolder;

    @Getter
    private long lastRunMillis;

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

}
