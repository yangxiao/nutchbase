package org.apache.nutch.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

public class NutchSerializerFactory {
  public static final String SERIALIZER_KEY = "nutch.serializer.class";
  
  @SuppressWarnings("unchecked")
  public static <K, R extends NutchTableRow> NutchSerializer<K, R> create(Configuration conf)
  throws ClassNotFoundException {
    String className = conf.get(SERIALIZER_KEY,
        HbaseSerializer.class.getCanonicalName());
    Class<? extends NutchSerializer<K, R>> clazz =
      (Class<? extends NutchSerializer<K, R>>)
        NutchSerializerFactory.class.getClassLoader().loadClass(className);
    return ReflectionUtils.newInstance(clazz, conf);
  }
}
