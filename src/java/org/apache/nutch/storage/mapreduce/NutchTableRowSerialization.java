package org.apache.nutch.storage.mapreduce;

import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.nutch.storage.NutchTableRow;

public class NutchTableRowSerialization
implements Serialization<NutchTableRow> {

  @Override
  public boolean accept(Class<?> c) {
    return c.isAssignableFrom(NutchTableRow.class);
  }

  @Override
  public Deserializer<NutchTableRow> getDeserializer(Class<NutchTableRow> c) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Serializer<NutchTableRow> getSerializer(Class<NutchTableRow> c) {
    return new NutchTableRowSerializer();
  }

}
