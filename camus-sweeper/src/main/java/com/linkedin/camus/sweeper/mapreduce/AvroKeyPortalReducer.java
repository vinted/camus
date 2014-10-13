package com.linkedin.camus.sweeper.mapreduce;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.avro.mapreduce.AvroMultipleOutputs;

public class AvroKeyPortalReducer extends
    Reducer<AvroKey<GenericRecord>, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable> {
  private AvroKey<GenericRecord> outKey;
  private AvroMultipleOutputs amos;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    outKey = new AvroKey<GenericRecord>();
    amos = new AvroMultipleOutputs(context);
  }

  @Override
  protected void reduce(AvroKey<GenericRecord> key, Iterable<AvroValue<GenericRecord>> values, Context context)
      throws IOException, InterruptedException {
    int numVals = 0;

    for (AvroValue<GenericRecord> av : values) {
      outKey.datum(av.datum());
      numVals++;
    }

    if (numVals > 1) {
      context.getCounter("EventCounter", "More_Than_1").increment(1);
      context.getCounter("EventCounter", "Deduped").increment(numVals - 1);
    }

    String namedOutputKey = namedOutputForRecord(outKey.datum());
    String partitionPath = partitionPathForRecord(outKey.datum());

    amos.write(namedOutputKey, outKey, NullWritable.get(), partitionPath);
  }

  @Override
  public void cleanup(Reducer.Context context) throws IOException, java.lang.InterruptedException {
    amos.close();
    super.cleanup(context);
  }

  /*
   * Named output formatter
   */

  private String namedOutputForRecord(GenericRecord avroRecord) {
    String portal = avroRecord.get("portal").toString();
    // Named outputs can contain only alpha numeric chars
    return portal.replaceAll("[^A-Za-z0-9,]", "");
  }

  /*
   * Denotes how the files will be split in the job output directory.
   * Default naming prefixes files with "part" prefix.
   * "country=dev/test" would create directory "country=dev" and prefix
   * output files with "test" prefix. End result might look like:
   *
   * daily/year=2014/month=10/day=10/country=dev/test-r-00000.avro
   * daily/year=2014/month=10/day=10/country=dev/test-r-00001.avro
   */
  private String partitionPathForRecord(GenericRecord avroRecord) {
      String portal = avroRecord.get("portal").toString();
      return "country=" + portal + "/" + portal;
  }
}
