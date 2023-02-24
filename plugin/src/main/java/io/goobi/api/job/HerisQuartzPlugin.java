package io.goobi.api.job;

import org.goobi.production.flow.jobs.AbstractGoobiJob;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class HerisQuartzPlugin extends AbstractGoobiJob {
    @Override
    public void execute() {
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
        return "HerisJob";
    }

}
