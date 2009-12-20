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
import org.apache.nutch.storage.NutchHashMap;

@SuppressWarnings("all")
public class WebTableRow extends NutchTableRow implements SpecificRecord {
  public static final Schema _SCHEMA = Schema.parse("{\"type\":\"record\",\"name\":\"WebTableRow\",\"namespace\":\"org.apache.nutch.storage\",\"fields\":[{\"name\":\"status\",\"type\":\"int\"},{\"name\":\"fetchTime\",\"type\":\"long\"},{\"name\":\"fetchInterval\",\"type\":\"int\"},{\"name\":\"retriesSinceFetch\",\"type\":\"int\"},{\"name\":\"title\",\"type\":\"string\"},{\"name\":\"text\",\"type\":\"string\"},{\"name\":\"score\",\"type\":\"float\"},{\"name\":\"outlinks\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},{\"name\":\"metadata\",\"type\":{\"type\":\"map\",\"values\":\"bytes\"}}]}");
  private int status;
  private long fetchTime;
  private int fetchInterval;
  private int retriesSinceFetch;
  private Utf8 title;
  private Utf8 text;
  private float score;
  private Map<Utf8,Utf8> outlinks;
  private Map<Utf8,ByteBuffer> metadata;
  public Schema getSchema() { return _SCHEMA; }
  public Object get(int _field) {
    switch (_field) {
    case 0: return status;
    case 1: return fetchTime;
    case 2: return fetchInterval;
    case 3: return retriesSinceFetch;
    case 4: return title;
    case 5: return text;
    case 6: return score;
    case 7: return outlinks;
    case 8: return metadata;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void set(int _field, Object _value) {
    setFieldChanged(_field);
    switch (_field) {
    case 0:status = (Integer)_value; break;
    case 1:fetchTime = (Long)_value; break;
    case 2:fetchInterval = (Integer)_value; break;
    case 3:retriesSinceFetch = (Integer)_value; break;
    case 4:title = (Utf8)_value; break;
    case 5:text = (Utf8)_value; break;
    case 6:score = (Float)_value; break;
    case 7:outlinks = (Map<Utf8,Utf8>)_value; break;
    case 8:metadata = (Map<Utf8,ByteBuffer>)_value; break;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
  public int getStatus() {
    return (Integer) get(0);
  }
  public void setStatus(int value) {
    set(0, value);
  }
  public long getFetchTime() {
    return (Long) get(1);
  }
  public void setFetchTime(long value) {
    set(1, value);
  }
  public int getFetchInterval() {
    return (Integer) get(2);
  }
  public void setFetchInterval(int value) {
    set(2, value);
  }
  public int getRetriesSinceFetch() {
    return (Integer) get(3);
  }
  public void setRetriesSinceFetch(int value) {
    set(3, value);
  }
  public float getScore() {
    return (Float) get(6);
  }
  public void setScore(float value) {
    set(6, value);
  }
  public Utf8 getFromOutlinks(Utf8 key) {
    if (outlinks == null) { return null; }
    return outlinks.get(key);
  }
  public void putToOutlinks(Utf8 key, Utf8 value) {
    if (outlinks == null) {
      outlinks = new NutchHashMap<Utf8,Utf8>();
    }
    setFieldChanged(7);
    outlinks.put(key, value);
  }
  public Utf8 removeFromOutlinks(Utf8 key) {
    if (outlinks == null) { return null; }
    setFieldChanged(7);
    return outlinks.remove(key);
  }
  public ByteBuffer getFromMetadata(Utf8 key) {
    if (metadata == null) { return null; }
    return metadata.get(key);
  }
  public void putToMetadata(Utf8 key, ByteBuffer value) {
    if (metadata == null) {
      metadata = new NutchHashMap<Utf8,ByteBuffer>();
    }
    setFieldChanged(8);
    metadata.put(key, value);
  }
  public ByteBuffer removeFromMetadata(Utf8 key) {
    if (metadata == null) { return null; }
    setFieldChanged(8);
    return metadata.remove(key);
  }
   // O(n)... TODO: Find a better implementation
  public boolean has(String fieldName) {
    int i = 0;
    for (Map.Entry<String, Schema> field : getSchema().getFieldSchemas()) {
      if (field.getKey().equals(fieldName)) { return isFieldReadable(i); }
      i++;
    }
    throw new AvroRuntimeException("No Such field");
  }
}
