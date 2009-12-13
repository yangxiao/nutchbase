package org.apache.nutch.storage.mapreduce;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.nutch.storage.NutchTableRow;

public class NutchTableRowSerializer
implements Serializer<NutchTableRow> {

  private SpecificDatumWriter writer = new SpecificDatumWriter();

  private BinaryEncoder encoder;

  @Override
  public void close() throws IOException {  }

  @Override
  public void open(OutputStream out) throws IOException {
    encoder = new BinaryEncoder(out);
  }

  @Override
  public void serialize(NutchTableRow t) throws IOException {
    writer.setSchema(t.getSchema());
    writer.write(t, encoder);
  }
}
