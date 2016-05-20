package com.linkedin.camus.etl.kafka.partitioner;

import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.Partitioner;
import com.linkedin.camus.etl.kafka.common.DateUtils;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

import com.linkedin.camus.etl.kafka.partitioner.DefaultPartitioner;

public class VintedPartitioner extends DefaultPartitioner {
    public static final Text PORTAL = new Text("portal");
    public static final String ETL_PORTAL_TIMEZONE = "etl.timezones";

    public class MissingPortalTimeZoneException extends RuntimeException {
        public MissingPortalTimeZoneException(String message) {
            super(message);
        }
    }

    @Override
    public String encodePartition(JobContext context, IEtlKey key) throws MissingPortalTimeZoneException {
        long outfilePartitionMs = EtlMultiOutputFormat.getEtlOutputFileTimePartitionMins(context) * 60000L;
        String portal = key.getPartitionMap().get(PORTAL).toString();
        // get output date formatter for specific portal
        String portalTimeZone = context.getConfiguration().get(ETL_PORTAL_TIMEZONE + "." + portal);

        if (portalTimeZone == null) {
            throw new MissingPortalTimeZoneException("Missing timezone configuration for portal=" + portal);
        }

        DateTimeFormatter outputDateFormatter = DateUtils.getDateTimeFormatter(
            OUTPUT_DATE_FORMAT,
            DateTimeZone.forID(portalTimeZone)
        );

        return "" + getPortalPartition(outfilePartitionMs, key.getTime(), outputDateFormatter.getZone());
    }

    private long getPortalPartition(long timeGranularityMs, long timestamp, DateTimeZone outputDateTimeZone) {
        long adjustedTimeStamp = outputDateTimeZone.convertUTCToLocal(timestamp);
        long partitionedTime = (adjustedTimeStamp / timeGranularityMs) * timeGranularityMs;
        return partitionedTime;
    }
}
