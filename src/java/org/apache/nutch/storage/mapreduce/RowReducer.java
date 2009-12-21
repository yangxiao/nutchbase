package org.apache.nutch.storage.mapreduce;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.nutch.storage.NutchTableRow;

public class RowReducer<K1, V1, K2, V2 extends NutchTableRow>
extends Reducer<K1, V1, K2, V2> {

  public static <K1, V1, K2, V2 extends NutchTableRow>
  void initRowReducerJob(Job job,
      Class<? extends RowReducer<K1, V1, K2, V2>> reducerClass) {
    initRowReducerJob(job, reducerClass, null);
  }

  public static <K1, V1, K2, V2 extends NutchTableRow>
  void initRowReducerJob(Job job,
      Class<? extends RowReducer<K1, V1, K2, V2>> reducerClass,
      Class<? extends Partitioner<K1, V1>> partitionerClass) {
    job.getConfiguration().setStrings("io.serializations", 
        "org.apache.hadoop.io.serializer.WritableSerialization",
        StringSerialization.class.getCanonicalName(),
        NutchTableRowSerialization.class.getCanonicalName());
    job.setOutputFormatClass(RowOutputFormat.class);
    job.setReducerClass(reducerClass);
    if (partitionerClass != null) {
      job.setPartitionerClass(partitionerClass);
    }
  }
}
