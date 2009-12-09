package org.apache.nutch.storage;

import java.util.BitSet;

import org.apache.avro.specific.SpecificRecordBase;

public abstract class NutchTableRow extends SpecificRecordBase {
  private BitSet readBits;
  private BitSet changedBits;
  
  protected NutchTableRow() {
    changedBits = new BitSet(getSchema().getFields().size());
    readBits = new BitSet(getSchema().getFields().size());
  }
  
  protected void setFieldChanged(int fieldNum) {
    changedBits.set(fieldNum);
    readBits.set(fieldNum);
  }
  
  public boolean isFieldChanged(int fieldNum) {
    return changedBits.get(fieldNum);
  }

  public boolean isFieldReadable(int fieldNum) {
    return readBits.get(fieldNum);
  }
  
  /* package */ void clearChangedBits() {
    changedBits.clear();
  }
}
