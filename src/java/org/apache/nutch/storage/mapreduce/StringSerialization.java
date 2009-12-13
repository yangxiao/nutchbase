package org.apache.nutch.storage.mapreduce;

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
    return null;
  }

  @Override
  public Serializer<String> getSerializer(Class<String> c) {
    return new StringSerializer();
  }
}
