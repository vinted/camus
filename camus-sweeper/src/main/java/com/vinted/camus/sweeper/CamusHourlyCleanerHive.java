package com.vinted.camus.sweeper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;

import com.linkedin.camus.sweeper.utils.DateUtils;
import com.linkedin.camus.sweeper.utils.Utils;

public class CamusHourlyCleanerHive extends Configured implements Tool {
    public static final String SIMULATE = "camus.sweeper.clean.simulate";
    public static final String KAFKA_FORCE_DELETE = "kafka.force.delete";

    private Properties props;
    private DateUtils dUtils;
    private DateTimeFormatter srcOutputDailyFormat;
    private DateTimeFormatter srcOutputMonthFormat;
    private DateTimeFormatter destOutputDailyFormat;
    private DateTimeFormatter destOutputMonthFormat;
    private String country;

    private FileSystem sourceFS;

    private Path sourcePath;

    private Integer numDays;
    private Integer lookbackDays;
    private Integer defaultNumDays = 14;
    private Integer defaultLookbackDays = 30;

    private Boolean simulate;
    private Boolean defaultSimulate = false;
    private Boolean forceDelete;
    private Boolean defaultForceDelete = false;

    private String hourlySubdir;
    private String dailySubdir;

    private static Logger log = Logger.getLogger(CamusHourlyCleanerHive.class);

    public CamusHourlyCleanerHive() {
        this.props = new Properties();
    }

    public CamusHourlyCleanerHive(Properties props) {
        this.props = props;
        this.country = props.getProperty("camus.sweeper.clean.country");
        this.dUtils = new DateUtils(props);
        this.srcOutputDailyFormat = dUtils.getDateTimeFormatter("YYYY/MM/dd");
        this.srcOutputMonthFormat = dUtils.getDateTimeFormatter("YYYY/MM");
    }

    public static void main(String args[]) throws Exception {
        CamusHourlyCleanerHive job = new CamusHourlyCleanerHive();
        ToolRunner.run(job, args);
    }

    private String getCountry() {
        if (country != null) {
            return country;
        } else {
            throw new RuntimeException("CamusHourlyCleanerHive expects property camus.sweeper.clean.country to be set");
        }
    }

    public void run() throws Exception {
        log.info("Cleaning");

        Configuration srcConf = new Configuration();

        sourceFS = FileSystem.get(srcConf);

        String fromLocation = (String) props.getProperty("camus.sweeper.source.dir");
        String destLocation = (String) props.getProperty("camus.sweeper.dest.dir", "");

        hourlySubdir = (String) props.getProperty("camus.sweeper.hourly.subdir", "hourly");
        dailySubdir = (String) props.getProperty("camus.sweeper.daily.subdir", "daily");

        if (destLocation.isEmpty()) {
            destLocation = fromLocation;
        }

        sourcePath = new Path(destLocation);

        numDays = Integer.parseInt((String) props.getProperty("camus.sweeper.clean.retention.hourly.num.days", defaultNumDays.toString()));
        lookbackDays = Integer.parseInt((String) props.getProperty("camus.sweeper.clean.hourly.lookback.days", defaultLookbackDays.toString()));

        simulate = Boolean.parseBoolean(props.getProperty(SIMULATE, defaultSimulate.toString()));
        forceDelete = Boolean.parseBoolean(props.getProperty(KAFKA_FORCE_DELETE, defaultForceDelete.toString()));

        destOutputDailyFormat = dUtils.getDateTimeFormatter("'year='YYYY/'month='MM/'day'=dd");
        destOutputMonthFormat = dUtils.getDateTimeFormatter("'year='YYYY/'month='MM");

        srcOutputDailyFormat = dUtils.getDateTimeFormatter("YYYY/MM/dd");
        srcOutputMonthFormat = dUtils.getDateTimeFormatter("YYYY/MM");

        for (FileStatus status : sourceFS.listStatus(sourcePath)) {
            String name = status.getPath().getName();
            if (name.startsWith(".") || name.startsWith("_")) {
                continue;
            }

            iterateTopic(name);
        }
    }

    private void iterateTopic(String topic) throws IOException {
        log.info("Cleaning up topic " + topic);

        DateTime time = new DateTime(dUtils.zone);
        DateTime daysAgo = time.minusDays(numDays);

        DateTime currentTime = time.minusDays(lookbackDays);
        int currentMonth = currentTime.getMonthOfYear();

        while (currentTime.isBefore(daysAgo)) {
            String srcDateString = srcOutputDailyFormat.print(currentTime);
            String destDateString = destOutputDailyFormat.print(currentTime);
            Path sourceHourlyDate = new Path(sourcePath, topic + "/" + hourlySubdir + "/" + srcDateString);
            Path sourceDailyDate = new Path(sourcePath, topic + "/" + dailySubdir + "/" + destDateString);
            log.info(sourceHourlyDate);

            if (sourceFS.exists(sourceHourlyDate)) {
                log.info("Hourly data exists for " + sourceHourlyDate.toString());
                if (sourceFS.exists(sourceDailyDate) || forceDelete) {
                    log.info("Deleting " + sourceHourlyDate);
                    // We should be sure that if this source is deleted that the destinations were also
                    // cleared out too.
                    if (!simulate && !sourceFS.delete(sourceHourlyDate, true)) {
                        throw new IOException("Error deleting " + sourceHourlyDate + " on " + sourceFS.getUri());
                    }
                } else {
                    throw new IOException("Daily data for " + sourceHourlyDate + " doesn't exist!");
                }
            }

            DateTime newTime = currentTime.plusDays(1);
            if (newTime.getMonthOfYear() != currentMonth) {
                log.info("Checking month to see if we need to clean up");
                Path monthPath = new Path(sourcePath, topic + "/" + hourlySubdir + "/" + srcOutputMonthFormat.print(currentTime));

                if (sourceFS.exists(monthPath)) {
                    FileStatus[] status = sourceFS.listStatus(monthPath);
                    if (!simulate && status != null && status.length == 0) {
                        log.info("Deleting " + monthPath);
                        sourceFS.delete(monthPath, true);
                    }
                }

                currentMonth = newTime.getMonthOfYear();
            }

            currentTime = newTime;
        }
    }

    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption("p", true, "properties filename from the classpath");
        options.addOption("P", true, "external properties filename");

        options.addOption(OptionBuilder.withArgName("property=value")
                          .hasArgs(2)
                          .withValueSeparator()
                          .withDescription("use value for given property")
                          .create("D"));

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);

        if (!(cmd.hasOption('p') || cmd.hasOption('P'))) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("CamusJob.java", options);
            return 1;
        }

        if (cmd.hasOption('p')) {
            props.load(ClassLoader.getSystemClassLoader().getResourceAsStream(cmd.getOptionValue('p')));
        }

        if (cmd.hasOption('P')) {
            File file = new File(cmd.getOptionValue('P'));
            FileInputStream fStream = new FileInputStream(file);
            props.load(fStream);
        }

        props.putAll(cmd.getOptionProperties("D"));

        dUtils = new DateUtils(props);
        srcOutputDailyFormat = dUtils.getDateTimeFormatter("YYYY/MM/dd");
        srcOutputMonthFormat = dUtils.getDateTimeFormatter("YYYY/MM");

        run();
        return 0;
    }
}
