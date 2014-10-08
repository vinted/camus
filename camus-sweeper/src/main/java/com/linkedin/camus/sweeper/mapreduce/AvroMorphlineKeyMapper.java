package com.linkedin.camus.sweeper.mapreduce;

import java.io.IOException;
import java.io.File;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Configs;
import org.kitesdk.morphline.base.Fields;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import com.typesafe.config.ConfigResolveOptions;

public class AvroMorphlineKeyMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, Object>
{
    private AvroKey<GenericRecord> outKey;
    private AvroValue<GenericRecord> outValue;
    private Schema keySchema;
    private String topic;
    private String morphlineConfiguration;
    private String portal;

    private static final class RecordEmitter implements Command {
        private final Mapper.Context context;
        private final AvroKey<GenericRecord> outKey;
        private final String portal;

        private RecordEmitter(Mapper.Context context, AvroKey<GenericRecord> outKey, String portal) {
            this.context = context;
            this.outKey = outKey;
            this.portal = portal;
        }

        @Override
        public void notify(Record notification) {
        }

        @Override
        public Command getParent() {
            return null;
        }

        @Override
        public boolean process(Record record) {
            GenericRecord avroRecord = (GenericRecord) record.get("_attachment_body").get(0);
            AvroValue<GenericRecord> outValue = new AvroValue<GenericRecord>();

            try {
                if (portal.equals((String) record.get("/portal").get(0))) {
                    outValue.datum(avroRecord);
                    projectData(avroRecord, outKey.datum());
                    context.write(outKey, outValue);
                }
            } catch (Exception e) {
                throw new RuntimeException("Cannot write record to context " + e);
            }

            return true;
        }

        private void projectData(GenericRecord source, GenericRecord target) {
            for (Field fld : target.getSchema().getFields()) {
                    if (fld.schema().getType() == Type.UNION) {
                            Object obj = source.get(fld.name());
                            Schema sourceSchema = GenericData.get().induce(obj);
                            if (sourceSchema.getType() == Type.RECORD){
                                for (Schema type : fld.schema().getTypes()){
                                    if (type.getFullName().equals(sourceSchema.getFullName())) {
                                        GenericRecord record = new GenericData.Record(type);
                                        target.put(fld.name(), record);
                                        projectData((GenericRecord) obj, record);
                                        break;
                                    }
                                }
                            } else {
                                target.put(fld.name(), source.get(fld.name()));
                            }
                        } else if (fld.schema().getType() == Type.RECORD) {
                            GenericRecord record = (GenericRecord) target.get(fld.name());

                            if (record == null){
                                record = new GenericData.Record(fld.schema());
                                target.put(fld.name(), record);
                            }

                            projectData((GenericRecord) source.get(fld.name()), record);
                        } else {
                            target.put(fld.name(), source.get(fld.name()));
                        }
                }
        }
    }

    private final Record record = new Record();
    private Command morphline;
    private Config  morphlineConfig;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
            Config morphlineTopicConfig = null;
            keySchema = AvroJob.getMapOutputKeySchema(context.getConfiguration());
            System.out.println("output schema: " + keySchema);
            topic = context.getConfiguration().get("camus.sweeper.morphlines.topic");
            portal = context.getConfiguration().get("portal");
            morphlineConfiguration = context.getConfiguration().get("camus.sweeper.morphlines.configuration");

            outValue = new AvroValue<GenericRecord>();
            outKey = new AvroKey<GenericRecord>();
            outKey.datum(new GenericData.Record(keySchema));

            RecordEmitter recordEmitter = new RecordEmitter(context, outKey, portal);
            MorphlineContext morphlineContext = new MorphlineContext.Builder().build();

            morphlineConfig = ConfigFactory.parseString(morphlineConfiguration).getConfig("morphline");

            morphline = new org.kitesdk.morphline.base.Compiler().compile(morphlineConfig, morphlineContext, recordEmitter);
    }

    @Override
    protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException,
    InterruptedException {
            record.put(Fields.ATTACHMENT_BODY, key.datum());
            if (!morphline.process(record)) {
                // do not allow processing to continue if morphline fails
                throw new RuntimeException("Morphline failed to process record: " + record + " with morphline config: " + morphlineConfig);
            }
            record.removeAll(Fields.ATTACHMENT_BODY);
    }
}
