package org.apache.nutch.storage;

import java.util.BitSet;

import org.apache.avro.specific.SpecificRecord;

public abstract class NutchTableRow
implements SpecificRecord {
  protected BitSet changedBits;
  protected BitSet readableBits;
  
  protected NutchTableRow() {
    changedBits = new BitSet(getSchema().getFields().size());
    readableBits = new BitSet(getSchema().getFields().size());
  }

  public abstract boolean has(String fieldName);
}
