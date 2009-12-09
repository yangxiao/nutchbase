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

@SuppressWarnings("all")
public class WebTableRow extends NutchTableRow implements SpecificRecord {
  public static final Schema _SCHEMA = Schema.parse("{\"type\":\"record\",\"name\":\"WebTableRow\",\"namespace\":\"org.apache.nutch.storage\",\"fields\":[{\"name\":\"fetchTime\",\"type\":\"long\"},{\"name\":\"title\",\"type\":\"string\"},{\"name\":\"text\",\"type\":\"string\"},{\"name\":\"status\",\"type\":\"int\"}]}");
  private long fetchTime;
  private Utf8 title;
  private Utf8 text;
  private int status;
  public Schema getSchema() { return _SCHEMA; }
  public Object get(int _field) {
    switch (_field) {
    case 0: return fetchTime;
    case 1: return title;
    case 2: return text;
    case 3: return status;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void set(int _field, Object _value) {
    switch (_field) {
    case 0: setFieldChanged(0);fetchTime = (Long)_value; break;
    case 1: setFieldChanged(1);title = (Utf8)_value; break;
    case 2: setFieldChanged(2);text = (Utf8)_value; break;
    case 3: setFieldChanged(3);status = (Integer)_value; break;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
  public long getFetchTime() {
    return (Long) get(0);
  }
  public void setFetchTime(long value) {
    set(0, value);
  }
  public Utf8 getTitle() {
    return (Utf8) get(1);
  }
  public void setTitle(Utf8 value) {
    set(1, value);
  }
  public Utf8 getText() {
    return (Utf8) get(2);
  }
  public void setText(Utf8 value) {
    set(2, value);
  }
  public int getStatus() {
    return (Integer) get(3);
  }
  public void setStatus(int value) {
    set(3, value);
  }
}
