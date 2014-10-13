package com.linkedin.camus.sweeper.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import java.net.URI;

import org.apache.http.HttpResponse;
import org.apache.http.HttpEntity;
import org.apache.http.util.EntityUtils;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.client.methods.HttpGet;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.log4j.Logger;

public class CamusMorphlineAvroKeyJob extends CamusSweeperJob
{
    // see http://hc.apache.org/httpclient-3.x/preference-api.html
    // for more info on timeouts
    private Integer httpConnectionTimeout;
    private Integer httpSocketTimeout;
    private Integer defaultHttpConnectionTimeout = 30000; // in milliseconds
    private Integer defaultHttpSocketTimeout = 0; // infininite

    @Override
    public void configureJob(String topic, Job job)
    {
        // setting up our input format and map output types
        super.configureInput(job, AvroKeyCombineFileInputFormat.class, AvroMorphlineKeyMapper.class, AvroKey.class, AvroValue.class);

        // setting up our output format and output types
        super.configureOutput(job, AvroKeyOutputFormat.class, AvroKeyPortalReducer.class, AvroKey.class, NullWritable.class);

        // if all output gets sent to NamedOutputs don't leave the empty part-00000 file
        // in the root of output directory
        // more info http://hadoop.apache.org/docs/r2.3.0/api/org/apache/hadoop/mapreduce/lib/output/MultipleOutputs.html
        LazyOutputFormat.setOutputFormatClass(job, AvroKeyOutputFormat.class);

        // set http connection timeouts
        httpConnectionTimeout = Integer.parseInt(getConfValue(job, topic, "camus.sweeper.schema.registry.http.connection.timeout", defaultHttpConnectionTimeout.toString()));
        httpSocketTimeout = Integer.parseInt(getConfValue(job, topic, "camus.sweeper.schema.registry.http.connection.socket.timeout", defaultHttpSocketTimeout.toString()));

        log.info("Schema registry HTTP Connection timeout set to: " + httpConnectionTimeout);
        log.info("Schema registry HTTP Connection socket timeout set to: " + httpSocketTimeout);

        // finding the newest file from our input. this file will contain the newest version of our avro
        // schema.
        Schema schema = getNewestInputSchemaFromSource(job, topic);

        // checking if we have a key schema used for deduping. if we don't then we make this a map only
        // job and set the key schema
        // to the newest input schema
        String keySchemaStr = getConfValue(job, topic, "camus.sweeper.avro.key.schema");
        Schema keySchema;
        if (keySchemaStr == null || keySchemaStr.isEmpty()) {
            job.setNumReduceTasks(0);
            keySchema = schema;
        } else {
            keySchema = new Schema.Parser().parse(keySchemaStr);
        }

        setupSchemas(topic, job, schema, keySchema);

        try {
            String schemaRegistryHost = getConfValue(job, topic, "camus.sweeper.schema.registry.host");
            URI morphlinesURI = new URI(schemaRegistryHost + "/" + topic + "/latest.morphlines");
            log.info("Fetching latest morphlines from URI " + morphlinesURI);
            String latestMorphlinePayload = fetchContentFromURI(morphlinesURI);
            String latestMorphline = latestMorphlinePayload.split("\t")[1];
            job.getConfiguration().set("camus.sweeper.morphlines.configuration", latestMorphline);
            job.getConfiguration().set("camus.sweeper.morphlines.topic", topic);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (java.net.URISyntaxException e) {
            throw new RuntimeException(e);
        }

        // setting the compression level. Only used if compression is enabled. default is 6
        job.getConfiguration().setInt(AvroOutputFormat.DEFLATE_LEVEL_KEY,
                                      job.getConfiguration().getInt(AvroOutputFormat.DEFLATE_LEVEL_KEY, 6));
    }

    private void setupSchemas(String topic, Job job, Schema schema, Schema keySchema) {
        log.info("Input Schema set to " + schema.toString());
        AvroJob.setInputKeySchema(job, schema);

        AvroJob.setMapOutputKeySchema(job, keySchema);

        Schema reducerSchema = getNewestOutputSchemaFromSource(job, topic);

        setupNamedOutputs(topic, job, schema, reducerSchema);

        AvroJob.setMapOutputValueSchema(job, reducerSchema);
        AvroJob.setOutputKeySchema(job, reducerSchema);
        log.info("Output Schema set to " + reducerSchema.toString());
    }

    private void setupNamedOutputs(String topic, Job job, Schema schema, Schema reducerSchema) {
        // Named outputs can contain only alpha numeric chars
        String portalList = getConfValue(job, topic, "camus.sweeper.portals").replaceAll("[^A-Za-z0-9,]", "");
        String[] portals = portalList.split(",");

        for (String portal : portals) {
            AvroMultipleOutputs.addNamedOutput(job, portal, AvroKeyOutputFormat.class, reducerSchema);
            log.info("Adding named output for potal: " + portal);
        }
    }

    private Schema getNewestInputSchemaFromSource(Job job, String topic)
    {
        return getNewestSchemaFromSource(job, topic, null);
    }

    private Schema getNewestOutputSchemaFromSource(Job job, String topic)
    {
        String destinationSchemaFormat = getConfValue(job, topic, "camus.sweeper.destination.schema.format");

        return getNewestSchemaFromSource(job, topic, destinationSchemaFormat);
    }


    private Schema getNewestSchemaFromSource(Job job, String topic, String destinationSchemaFormat) {
        URI schemaURI;
        String schemaRegistryHost = getConfValue(job, topic, "camus.sweeper.schema.registry.host");

        try {
            if (destinationSchemaFormat != null) {
                schemaURI = new URI(schemaRegistryHost + "/" + topic + "/latest." + destinationSchemaFormat);
            } else {
                schemaURI = new URI(schemaRegistryHost + "/" + topic + "/latest");
            }
            log.info("Fetching latest schema from " + schemaURI);
            String latestSchemaPayload = fetchContentFromURI(schemaURI);
            String latestSchema = latestSchemaPayload.split("\t")[1];
            return new Schema.Parser().parse(latestSchema);
        } catch (java.net.URISyntaxException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String fetchContentFromURI(URI address) throws IOException {
        DefaultHttpClient httpClient = new DefaultHttpClient();

        httpClient.getParams().setIntParameter("http.connection.timeout",
                                               httpConnectionTimeout);

        httpClient.getParams().setIntParameter("http.socket.timeout",
                                               httpSocketTimeout);

        HttpGet requestMethod = new HttpGet(address);
        HttpResponse httpResponse = httpClient.execute(requestMethod);
        HttpEntity responseEntity = httpResponse.getEntity();
        String response = EntityUtils.toString(responseEntity, "UTF-8");
        requestMethod.releaseConnection();
        return response;
    }
}
