package org.apache.nutch.storage;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;

public interface NutchSerializer<T extends NutchTableRow<K>, K> extends Configurable {
  public T readRow(String key, String[] fields) throws IOException;
  
  public void writeRow(T row) throws IOException;
}
