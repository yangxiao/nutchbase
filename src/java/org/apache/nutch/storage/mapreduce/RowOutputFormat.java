package org.apache.nutch.storage.mapreduce;

import java.io.IOException;

import org.apache.hadoop.hbase.mapreduce.TableOutputCommitter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.nutch.storage.NutchSerializer;
import org.apache.nutch.storage.NutchSerializerFactory;
import org.apache.nutch.storage.NutchTableRow;

public class RowOutputFormat<K, R extends NutchTableRow>
extends OutputFormat<K, R>{

  @Override
  public void checkOutputSpecs(JobContext context)
  throws IOException, InterruptedException { }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
  throws IOException, InterruptedException {
    return new TableOutputCommitter(); // doesn't do anything
  }

  @Override
  public RecordWriter<K, R> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    final NutchSerializer<K, R> serializer =
      NutchSerializerFactory.create(context.getConfiguration());
    return new RecordWriter<K, R>() {
      @Override
      public void close(TaskAttemptContext context) throws IOException,
          InterruptedException {
        serializer.sync();
      }

      @Override
      public void write(K key, R row)
      throws IOException, InterruptedException {
        serializer.updateRow(key, row);
      }
    };
  }

}
