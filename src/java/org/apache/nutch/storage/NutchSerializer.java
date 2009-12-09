package org.apache.nutch.storage;

import java.io.IOException;

public interface NutchSerializer<K, R extends NutchTableRow> {
  public void createTable() throws IOException;
  
  public R readRow(K key, String[] fields) throws IOException;
  
  public void writeRow(K key, R row) throws IOException;
}
