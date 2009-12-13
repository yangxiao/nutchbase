package org.apache.nutch.storage;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map.Entry;

public interface RowScanner<K, R extends NutchTableRow>
extends Closeable {

  public Entry<K, R> next() throws IOException;
}
