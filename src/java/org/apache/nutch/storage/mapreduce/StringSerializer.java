package org.apache.nutch.storage.mapreduce;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.serializer.Serializer;

public class StringSerializer implements Serializer<String> {

  private DataOutputStream out;

  @Override
  public void close() throws IOException {
    this.out.close();
  }

  @Override
  public void open(OutputStream out) throws IOException {
    this.out = new DataOutputStream(out);
  }

  @Override
  public void serialize(String t) throws IOException {
    Bytes.writeByteArray(out, Bytes.toBytes(t));
  }
}
