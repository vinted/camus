package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.partitioner.VintedPartitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class TestVintedPartitioner {

    @Test
    public void testGeneratePartitionPath() throws IOException {
        Configuration testConfiguration = new Configuration();
        Job testJob = new Job(new Configuration());

        VintedPartitioner testPartitioner = new VintedPartitioner();
        testPartitioner.setConf(testConfiguration);

        String actualResult = testPartitioner.generatePartitionedPath(testJob, "testTopic", "1406777693000");
        String expectedResult = "testTopic/hourly/2014/07/30/20";

        assertTrue(actualResult.equals(expectedResult));

        actualResult = testPartitioner.generateFileName(testJob, "testTopic", "testBrokerId", 123, 100, 500, "1406777693000");
        expectedResult = "testTopic.testBrokerId.123.100.500.1406777693000";

        assertTrue(actualResult.equals(expectedResult));
    }

    @Test(expected=VintedPartitioner.MissingPortalTimeZoneException.class) // missing portal time zone configuration for US
    public void testMissingPortalTimezone() throws IOException {
        EtlKey testEtlKey = new EtlKey();
        testEtlKey.setTime(1463640999383L);
        testEtlKey.put(new Text("portal"), new Text("us"));
        Configuration testConfiguration = new Configuration();
        Job testJob = new Job(new Configuration());

        VintedPartitioner testPartitioner = new VintedPartitioner();
        testPartitioner.setConf(testConfiguration);

        String actualResult = testPartitioner.encodePartition(testJob, testEtlKey);
    }

    @Test
    public void testEncodedPartition() throws IOException {
        EtlKey testEtlKey = new EtlKey();
        testEtlKey.setTime(1463640999383L); // 2016-05-19 06:56:39 UTC
        testEtlKey.put(new Text("portal"), new Text("us"));
        Configuration testConfiguration = new Configuration();
        Configuration jobConfiguration = new Configuration();
        jobConfiguration.set(VintedPartitioner.ETL_PORTAL_TIMEZONE + "." + "us", "Etc/GMT+7");
        Job testJob = new Job(jobConfiguration);

        VintedPartitioner testPartitioner = new VintedPartitioner();
        testPartitioner.setConf(testConfiguration);

        String actualResult = testPartitioner.encodePartition(testJob, testEtlKey);
        String expectedResult = "1463612400000"; // Wed, 18 May 2016 23:56:39 MST -07:00

        assertTrue(actualResult.equals(expectedResult));
    }
}
