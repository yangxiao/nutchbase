package org.apache.nutch.storage;

import java.util.BitSet;

import org.apache.avro.specific.SpecificRecordBase;

public abstract class NutchTableRow extends SpecificRecordBase {
  protected BitSet changedBits;
  protected BitSet readableBits;
  
  protected NutchTableRow() {
    changedBits = new BitSet(getSchema().getFields().size());
    readableBits = new BitSet(getSchema().getFields().size());
  }
}
