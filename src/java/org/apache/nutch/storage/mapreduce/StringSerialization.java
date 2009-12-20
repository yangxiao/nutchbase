package org.apache.nutch.storage.mapreduce;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

public class StringSerialization implements Serialization<String> {

  @Override
  public boolean accept(Class<?> c) {
    return c.getCanonicalName().equals(String.class.getCanonicalName());
  }

  @Override
  public Deserializer<String> getDeserializer(Class<String> c) {
    return new Deserializer<String>() {
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
    };
  }

  @Override
  public Serializer<String> getSerializer(Class<String> c) {
    return new Serializer<String>() {

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
      
    };
  }
}
