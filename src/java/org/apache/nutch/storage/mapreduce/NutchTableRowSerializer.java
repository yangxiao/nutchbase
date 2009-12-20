package org.apache.nutch.storage.mapreduce;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.nutch.storage.NutchHashMap;
import org.apache.nutch.storage.NutchTableRow;
import org.apache.nutch.storage.NutchTableRowInternal;
import org.apache.nutch.storage.NutchHashMap.State;

public class NutchTableRowSerializer extends SpecificDatumWriter
implements Serializer<NutchTableRow> {

  private BinaryEncoder encoder;

  @Override
  public void close() throws IOException {
    encoder.flush();
  }

  @Override
  public void open(OutputStream out) throws IOException {
    encoder = new BinaryEncoder(out);
  }

  @Override
  public void serialize(NutchTableRow row) throws IOException {
    NutchTableRowInternal t = (NutchTableRowInternal) row;
    
    setSchema(t.getSchema());

    for (Entry<String, Field> e : t.getSchema().getFields().entrySet()) {
      Field field = e.getValue();
      // TODO: This is extremely inefficient. Read and write bitsets
      // directly. Right now, a readable bit is unnecessarily an INTEGER.
      encoder.writeBoolean(t.isFieldReadable(field.pos()));
      encoder.writeBoolean(t.isFieldChanged(field.pos()));
      if (t.isFieldReadable(field.pos())) {
        Object o = t.get(field.pos());
        write(field.schema(), o, encoder);
        writeExtraInfo(o, field.schema(), encoder);

      }
    }
  }
  
  @SuppressWarnings("unchecked")
  private void writeExtraInfo(Object o, Schema schema, Encoder encoder)
  throws IOException {
    if (schema.getType() == Type.MAP) {
      // write extra state information for maps
      NutchHashMap<Utf8, ?> map = (NutchHashMap) o;
      encoder.writeInt(map.states().size());
      for (Entry<Utf8, State> e2 : map.states().entrySet()) {
        encoder.writeString(e2.getKey());
        encoder.writeInt(e2.getValue().ordinal());
      }
    }
  }
}
