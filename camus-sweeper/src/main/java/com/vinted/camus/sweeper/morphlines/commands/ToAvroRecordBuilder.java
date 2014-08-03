package com.vinted.camus.sweeper.morphlines.commands;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.reflect.ReflectData;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Configs;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.stdio.AbstractParser;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


/**
 * Command that converts a morphline record to an Avro record.
 */
public final class ToAvroRecordBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("toAvroRecord");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new ToAvroRecord(this, config, parent, child, context);
  }

  private static final class ToAvroRecord extends AbstractCommand {
    public ToAvroRecord(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);
    }

    @Override
    protected boolean doProcess(Record inputRecord) {
      Record outputRecord = new Record();

      /*
        extractAvroTree command extracts Avro record fields with "/" prefixes
        whereas toAvro command expects Morphlines record field names to start
        without the prefix, so simply remove it for convenience. Otherwise
        toAvro mappings key has to be passed with *all* the attributes listed
        for each record
      */

      for (String path : inputRecord.getFields().keySet()) {
        String flatPath = path;

        if (flatPath.startsWith("/")) {
          flatPath = flatPath.substring(1);
        }
        flatPath = flatPath.replace("/", "_");
        List values = inputRecord.get(path);

        if (flatPath.contains("[]")) {
          // if field contains more then one value it originated from
          // avro union, put all records back as an array
          outputRecord.put(flatPath.replace("[]", ""), values);
        } else {
          for (Object value : values) {
            outputRecord.put(flatPath, value);
          }
        }
      }

      return super.doProcess(outputRecord);
    }
  }
}
