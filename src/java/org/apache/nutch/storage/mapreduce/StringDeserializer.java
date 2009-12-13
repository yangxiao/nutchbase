package org.apache.nutch.storage.mapreduce;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.serializer.Deserializer;

public class StringDeserializer implements Deserializer<String> {

  private DataInputStream in;

  @Override
  public void open(InputStream in) throws IOException {
    this.in = new DataInputStream(in);
  }

  @Override
  public void close() throws IOException {
    this.in.close();
  }

  @Override
  public String deserialize(String t) throws IOException {
    return Bytes.toString(Bytes.readByteArray(in));
  }
}
