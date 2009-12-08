package org.apache.nutch.storage;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Protocol;
import org.apache.avro.util.Utf8;
import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.SpecificExceptionBase;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificFixed;
import org.apache.avro.reflect.FixedSize;
import org.apache.nutch.storage.RowKey;
import org.apache.nutch.storage.RowField;

@SuppressWarnings("all")
public class WebTableRow extends NutchTableRow< Utf8> implements SpecificRecord {
  public static final Schema _SCHEMA = Schema.parse("{\"type\":\"record\",\"name\":\"WebTableRow\",\"namespace\":\"org.apache.nutch.storage\",\"fields\":[{\"name\":\"rowKey\",\"type\":\"string\"},{\"name\":\"fetchTime\",\"type\":\"long\"},{\"name\":\"title\",\"type\":\"string\"},{\"name\":\"text\",\"type\":\"string\"},{\"name\":\"status\",\"type\":\"int\"}]}");
  @RowKey
  private Utf8 rowKey;
  @RowField
  private long fetchTime;
  @RowField
  private Utf8 title;
  @RowField
  private Utf8 text;
  @RowField
  private int status;
  public Schema getSchema() { return _SCHEMA; }
  public Object get(int _field) {
    switch (_field) {
    case 0: return rowKey;
    case 1: return fetchTime;
    case 2: return title;
    case 3: return text;
    case 4: return status;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void set(int _field, Object _value) {
    switch (_field) {
    case 0: setFieldChanged(0);rowKey = (Utf8)_value; break;
    case 1: setFieldChanged(1);fetchTime = (Long)_value; break;
    case 2: setFieldChanged(2);title = (Utf8)_value; break;
    case 3: setFieldChanged(3);text = (Utf8)_value; break;
    case 4: setFieldChanged(4);status = (Integer)_value; break;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
  public Utf8 getRowKey() {
    return (Utf8) get(0);
  }
  public void setRowKey(Utf8 value) {
    set(0, value);
  }
  public long getFetchTime() {
    return (Long) get(1);
  }
  public void setFetchTime(long value) {
    set(1, value);
  }
  public Utf8 getTitle() {
    return (Utf8) get(2);
  }
  public void setTitle(Utf8 value) {
    set(2, value);
  }
  public Utf8 getText() {
    return (Utf8) get(3);
  }
  public void setText(Utf8 value) {
    set(3, value);
  }
  public int getStatus() {
    return (Integer) get(4);
  }
  public void setStatus(int value) {
    set(4, value);
  }
}
