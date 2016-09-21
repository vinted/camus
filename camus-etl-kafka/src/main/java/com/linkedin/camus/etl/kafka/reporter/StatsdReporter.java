package com.linkedin.camus.etl.kafka.reporter;

import com.timgroup.statsd.StatsDClient;
import com.timgroup.statsd.NonBlockingStatsDClient;
import java.util.Map;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.util.ToolRunner;

import com.linkedin.camus.etl.kafka.reporter.TimeReporter;

public class StatsdReporter extends TimeReporter {

    public static final String STATSD_ENABLED = "statsd.enabled";
    public static final String STATSD_HOST = "statsd.host";
    public static final String STATSD_PORT = "statsd.port";
    public static final String STATSD_PREFIX = "statsd.prefix";
    public static final String STATSD_TAGS = "statsd.tags";

    public static final String DEFAULT_STATSD_PREFIX = "Camus";
    public static final String DEFAULT_STATSD_TAGS = "camus:counters"; // comma separated

    public void report(Job job, Map<String, Long> timingMap) throws IOException {
        super.report(job, timingMap);
        submitCountersToStatsd(job);
    }

    private void submitCountersToStatsd(Job job) throws IOException {
        Counters counters = job.getCounters();
        if (getStatsdEnabled(job)) {
            StatsDClient statsd = new NonBlockingStatsDClient(
                getStatsdPrefix(job), getStatsdHost(job), getStatsdPort(job), getStatsdTags(job)
            );

            for (CounterGroup counterGroup: counters) {
                for (Counter counter: counterGroup){
                    statsd.gauge(counterGroup.getDisplayName() + "." + counter.getDisplayName(), counter.getValue());
                }
            }
        }
    }

    public static Boolean getStatsdEnabled(Job job) {
        return job.getConfiguration().getBoolean(STATSD_ENABLED, false);
    }

    public static String getStatsdHost(Job job) {
        return job.getConfiguration().get(STATSD_HOST, "localhost");
    }

    public static int getStatsdPort(Job job) {
        return job.getConfiguration().getInt(STATSD_PORT, 8125);
    }

    public static String getStatsdPrefix(Job job) {
        return job.getConfiguration().get(STATSD_PREFIX, DEFAULT_STATSD_PREFIX);
    }

    public static String[] getStatsdTags(Job job) {
        String tags = job.getConfiguration().get(STATSD_TAGS, DEFAULT_STATSD_TAGS);

        if (tags.length() > 0) {
            return tags.split(",");
        } else {
            return new String[] {};
        }
    }
}
