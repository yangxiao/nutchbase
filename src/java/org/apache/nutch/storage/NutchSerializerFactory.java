package org.apache.nutch.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.nutch.storage.hbase.HbaseSerializer;

public class NutchSerializerFactory {
  public static final String SERIALIZER_KEY = "nutch.serializer.class";
  
  @SuppressWarnings("unchecked")
  public static <K, R extends NutchTableRow> NutchSerializer<K, R> create(Configuration conf) {
    String className = conf.get(SERIALIZER_KEY,
        HbaseSerializer.class.getCanonicalName());
    Class<? extends NutchSerializer<K, R>> clazz;
    try {
      clazz = (Class<? extends NutchSerializer<K, R>>)
        NutchSerializerFactory.class.getClassLoader().loadClass(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    return ReflectionUtils.newInstance(clazz, conf);
  }
}
