package org.apache.nutch.storage.mapreduce;

import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.nutch.storage.NutchTableRow;

public class NutchTableRowDeserializer
implements Deserializer<NutchTableRow> {

  private BinaryDecoder decoder;

  private SpecificDatumReader reader;
  
  @Override
  public void open(InputStream in) throws IOException {
    decoder = new BinaryDecoder(in);
    reader = new SpecificDatumReader();
  }

  @Override
  public void close() throws IOException { }

  @Override
  public NutchTableRow deserialize(NutchTableRow t)
  throws IOException {
    reader.setSchema(t.getSchema());
    return (NutchTableRow) reader.read(null, decoder);
  }
}
