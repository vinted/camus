package com.vinted.camus.sweeper.morphlines.commands;

import java.util.Collection;
import java.util.Collections;
import java.util.ListIterator;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;

import com.typesafe.config.Config;

public final class ToFloatBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("toFloat");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new ToFloat(this, config, parent, child, context);
  }

  private static final class ToFloat extends AbstractCommand {
    private final String fieldName;

    public ToFloat(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);
      this.fieldName = getConfigs().getString(config, "field");
      validateArguments();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean doProcess(Record record) {
      ListIterator iter = record.get(fieldName).listIterator();
      while (iter.hasNext()) {
        String str = iter.next().toString();
        iter.set(new Float(str));
      }

      return super.doProcess(record);
    }
  }
}
