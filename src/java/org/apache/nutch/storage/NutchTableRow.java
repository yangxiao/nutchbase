package org.apache.nutch.storage;

import java.util.BitSet;

import org.apache.avro.specific.SpecificRecordBase;

public abstract class NutchTableRow<T> extends SpecificRecordBase {
  private BitSet changedBits = new BitSet();
  
  protected void setFieldChanged(int fieldNum) {
    changedBits.set(fieldNum);
  }
  
  public boolean isFieldChanged(int fieldNum) {
    return changedBits.get(fieldNum);
  }
  
  public abstract T getRowKey();
}
