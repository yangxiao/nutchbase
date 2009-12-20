package org.apache.nutch.storage;

public abstract class NutchTableRowInternal extends NutchTableRow {
  
  // TODO: None of these should not be exposed as public. Find a better way...
  /** Do not call this method directly. This method is used internally and
   * calling this method will result in unpredictable behavior.
   */
  public void setFieldChanged(int fieldNum) {
    changedBits.set(fieldNum);
    readableBits.set(fieldNum);
  }

  public boolean isFieldChanged(int fieldNum) {
    return changedBits.get(fieldNum);
  }

  /** Do not call this method directly. This method is used internally and
   * calling this method will result in unpredictable behavior.
   */
  public void setFieldReadable(int fieldNum) {
    readableBits.set(fieldNum);
  }

  public boolean isFieldReadable(int fieldNum) {
    return readableBits.get(fieldNum);
  }

  /** Do not call this method directly. This method is used internally and
   * calling this method will result in unpredictable behavior.
   */
  public void clearChangedBits() {
    changedBits.clear();
  }
}
