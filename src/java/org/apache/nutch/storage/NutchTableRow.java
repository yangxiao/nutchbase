package org.apache.nutch.storage;

import java.util.BitSet;

import org.apache.avro.specific.SpecificRecordBase;

public abstract class NutchTableRow extends SpecificRecordBase {
  private BitSet changedBits;
  
  protected NutchTableRow() {
    changedBits = new BitSet(getSchema().getFields().size());
  }
  
  protected void setFieldChanged(int fieldNum) {
    changedBits.set(fieldNum);
  }
  
  public boolean isFieldChanged(int fieldNum) {
    return changedBits.get(fieldNum);
  }

  // TODO: This should not be exposed as public. Find a better way...
  public void clearChangedBits() {
    changedBits.clear();
  }
}
