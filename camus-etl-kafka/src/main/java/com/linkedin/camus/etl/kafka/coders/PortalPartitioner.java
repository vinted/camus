package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.Partitioner;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.kafka.common.DateUtils;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.apache.hadoop.io.Text;

public class PortalPartitioner extends Partitioner {

    protected static final String OUTPUT_DATE_FORMAT = "YYYY/MM/dd/HH";
    //protected DateTimeZone outputDateTimeZone = null;
    protected DateTimeFormatter outputDateFormatter = null;

    @Override
    public String encodePartition(JobContext context, IEtlKey key) {
        long outfilePartitionMs = EtlMultiOutputFormat.getEtlOutputFileTimePartitionMins(context) * 60000L;
        String portal = key.getPartitionMap().get(new Text("portal")).toString();
        return ""+portal+"_"+DateUtils.getPartition(outfilePartitionMs, key.getTime(), outputDateFormatter.getZone());
    }

    @Override
    public String generatePartitionedPath(JobContext context, String topic, String brokerId, int partitionId, String encodedParts) {
        StringBuilder sb = new StringBuilder();
        sb.append(topic).append("/");
        sb.append(EtlMultiOutputFormat.getDestPathTopicSubDir(context)).append("/");
        String[] splitParts = encodedParts.split("_");
        String portal = splitParts[0];
        String encodedPartition = splitParts[1];
        DateTime bucket = new DateTime(Long.valueOf(encodedPartition));
        sb.append(bucket.toString(outputDateFormatter));
        sb.append("/").append(portal);
        return sb.toString();
    }

    @Override
    public void setConf(Configuration conf)
    {
        if (conf != null){
        	outputDateFormatter = DateUtils.getDateTimeFormatter(OUTPUT_DATE_FORMAT,DateTimeZone.forID(conf.get(EtlMultiOutputFormat.ETL_DEFAULT_TIMEZONE, "America/Los_Angeles")));
        }

        super.setConf(conf);
    }
}
