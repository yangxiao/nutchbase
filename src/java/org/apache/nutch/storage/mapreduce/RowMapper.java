package org.apache.nutch.storage.mapreduce;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.nutch.storage.NutchTableRow;

public class RowMapper<K, R extends NutchTableRow>
extends Mapper<K, R, K, R> {

  public static <K, R extends NutchTableRow> void initRowMapperJob(Job job,
      Class<K> keyClass, Class<R> valueClass,
      Class<? extends RowMapper<K, R>> mapperClass, String... fields) {
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
}
