package org.apache.nutch.storage.mapreduce;

import java.util.Collection;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.nutch.storage.NutchTableRow;

public class RowMapper<K1, V1 extends NutchTableRow, K2, V2>
extends Mapper<K1, V1, K2, V2> {

  public static <K1, V1 extends NutchTableRow, K2, V2>
  void initRowMapperJob(Job job,Class<K2> keyClass, Class<V2> valueClass,
      Class<? extends RowMapper<K1, V1, K2, V2>> mapperClass, String... fields) {
    job.setInputFormatClass(RowInputFormat.class);
    job.setMapperClass(mapperClass);
    job.setMapOutputKeyClass(keyClass);
    job.setMapOutputValueClass(valueClass);
    job.getConfiguration().setStrings(RowInputFormat.MAPRED_FIELDS, fields);
    job.getConfiguration().setStrings("io.serializations", 
        "org.apache.hadoop.io.serializer.WritableSerialization",
        StringSerialization.class.getCanonicalName(),
        NutchTableRowSerialization.class.getCanonicalName());
  }
  
  public static <K1, V1 extends NutchTableRow, K2, V2>
  void initRowMapperJob(Job job,Class<K2> keyClass, Class<V2> valueClass,
      Class<? extends RowMapper<K1, V1, K2, V2>> mapperClass,
      Collection<String> fields) {
    initRowMapperJob(job, keyClass, valueClass, mapperClass,
        fields.toArray(new String[fields.size()]));
  }
}
