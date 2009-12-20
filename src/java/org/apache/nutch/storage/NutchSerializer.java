package org.apache.nutch.storage;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.nutch.plugin.Pluggable;

public interface NutchSerializer<K, R extends NutchTableRow> extends Pluggable {
  public void createTable() throws IOException;

  public R makeRow() throws IOException;
  
  public R readRow(K key, String[] fields) throws IOException;
  
  public void updateRow(K key, R row) throws IOException;

  public void sync() throws IOException;

  public void deleteRow(K key) throws IOException;

  public List<InputSplit> getSplits(K startRow, K stopRow, JobContext context)
  throws IOException;

  public RowScanner<K, R> makeScanner(K startRow, K stopRow, String[] fields)
  throws IOException;

  public RowScanner<K, R> makeScanner(InputSplit split, String[] fields)
  throws IOException;
}
