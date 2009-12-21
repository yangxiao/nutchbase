package org.apache.nutch.storage.mapreduce;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.nutch.storage.NutchHashMap;
import org.apache.nutch.storage.NutchTableRow;
import org.apache.nutch.storage.NutchTableRowInternal;
import org.apache.nutch.storage.NutchHashMap.State;

public class NutchTableRowDeserializer extends SpecificDatumReader
implements Deserializer<NutchTableRow> {

  private BinaryDecoder decoder;
  private Class<NutchTableRow> rowClass;

  public NutchTableRowDeserializer(Class<NutchTableRow> c) {
    this.rowClass = c;
  }
  
  @Override
  public void open(InputStream in) throws IOException {
    decoder = new BinaryDecoder(in);
  }

  @Override
  public void close() throws IOException { }

  @Override
  public NutchTableRow deserialize(NutchTableRow row)
  throws IOException {
    if (row == null) {
      try {
        row = rowClass.newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    NutchTableRowInternal t = (NutchTableRowInternal) row;
    setSchema(t.getSchema());

    Map<String, Field> fieldMap = t.getSchema().getFields();
    boolean[] changeds = new boolean[fieldMap.size()];

    int i = 0;
    for (Entry<String, Field> e : fieldMap.entrySet()) {
      boolean isReadable = decoder.readBoolean();
      changeds[i++] = decoder.readBoolean();
      Field field = e.getValue();
      if (isReadable) {
        Object o = read(null, field.schema(), field.schema(), decoder);
        t.set(field.pos(), o);
        readExtraInformation(field.schema(), o, decoder);
      }
    }

    // Now set changed bits
    t.clearChangedBits();
    for (i = 0; i < changeds.length; i++) {
      if (changeds[i]) {
        t.setFieldChanged(i);
      }
    }
    return t;
  }

  @SuppressWarnings("unchecked")
  private void readExtraInformation(Schema schema, Object o, Decoder decoder)
  throws IOException {
    if (schema.getType() == Type.MAP) {
      NutchHashMap<Utf8, ?> map = (NutchHashMap) o;
      map.resetStates();
      int size = decoder.readInt();
      for (int j = 0; j < size; j++) {
        Utf8 key = decoder.readString(null);
        State state = State.values()[decoder.readInt()];
        map.putState(key, state);
      }
    }    
  }
}
