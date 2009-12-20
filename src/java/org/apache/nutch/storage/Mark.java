package org.apache.nutch.storage;

import java.nio.ByteBuffer;

import org.apache.avro.util.Utf8;

public enum Mark {
  INJECT_MARK("_injmrk_");

  private Utf8 name;
  
  Mark(String name) {
    this.name = new Utf8(name);
  }

  public void putMark(WebTableRow row, byte[] markValue) {
    row.putToMetadata(name, ByteBuffer.wrap(markValue));
  }

  public void removeMark(WebTableRow row) {
    row.removeFromMetadata(name);
  }

  public byte[] checkMark(WebTableRow row) {
    ByteBuffer buffer = row.getFromMetadata(name);
    if (buffer == null) { return null; }
    return buffer.array();
  }
}
