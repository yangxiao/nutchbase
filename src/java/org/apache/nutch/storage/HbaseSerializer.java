package org.apache.nutch.storage;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Map.Entry;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.FactoryConfigurationError;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.nutch.util.NodeWalker;
import org.apache.nutch.util.NutchConfiguration;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

public class HbaseSerializer<T extends NutchTableRow<K>, K> extends Configured
implements NutchSerializer<T, K> {

  public static final String MAPPING_FILE_NAME_KEY = "mapping.hbase.file";
  
  public static final String DEFAULT_FILE_NAME = "hbase-mapping.xml";
  
  private static final DocumentBuilder docBuilder; 

  // a map from field name to hbase column
  private Map<String, HbaseColumn> columnMap;
  
  // field name to annotation map
  private Map<String, RowField> annMap;
  
  // a map from field name to its field
  private Map<String, Field> fieldMap;
  
  private String keyVarName; 
  
  private Class<?> keyType;
  
  private HTable table;
  
  private Class<T> implClass;
  
  static {
    try {
      docBuilder = 
        DocumentBuilderFactory.newInstance().newDocumentBuilder();
    } catch (ParserConfigurationException e) {
      throw new RuntimeException(e);
    } catch (FactoryConfigurationError e) {
      throw new RuntimeException(e);
    }
  }

  public HbaseSerializer(Configuration conf)  {
    super(conf);
    columnMap = new HashMap<String, HbaseColumn>();
    annMap = new HashMap<String, RowField>();
    fieldMap = new HashMap<String, Field>();
    String fileName = conf.get(MAPPING_FILE_NAME_KEY, DEFAULT_FILE_NAME);
    try {
      parseMapping(fileName);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public T readRow(String key, String[] fields) throws IOException {
    Get get = new Get(Bytes.toBytes(key));
    for (String f : fields) {
      HbaseColumn col = columnMap.get(f);
      Class<?> clazz = fieldMap.get(f).getType();
      if (Map.class.isAssignableFrom(clazz)) {
        // A map is serialized so that a key => value
        // mapping is represented with family:key => value.
        // So, to read a map, a column family is read with
        // all qualifiers
        get.addFamily(col.getFamily());
      } else {
        get.addColumn(col.getFamily(), col.getQualifier());
      }
    }
    Result result = table.get(get);
    try {
      return makeNutchTableRow(result, fields);
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (SecurityException e) {
      throw new RuntimeException(e);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeRow(T row) throws IOException {
    Schema schema = row.getSchema();
    int i = 0;
    Put put = new Put(toBytes(row.getRowKey()));
    for (Map.Entry<String, Schema> field : schema.getFieldSchemas()) {
      if (!row.isFieldChanged(i)) {
        System.out.println(field.getKey() + " is not changed, continuing");
        i++;
        continue;
      }
      if (field.getKey().equals("rowKey")) {
        throw new RuntimeException("Can't handle row key changes for now");
      }
      Object o = row.get(i);
      System.out.println("Writing " + o + " for field:" + field.getKey());
      HbaseColumn hcol = columnMap.get(field.getKey());
      put.add(hcol.getFamily(), hcol.getQualifier(), toBytes(o));
      i++;
    }
    table.put(put);
    table.flushCommits();
  }

  @SuppressWarnings("unchecked")
  private T makeNutchTableRow(Result result, String[] fields)
  throws InstantiationException, IllegalAccessException, SecurityException, NoSuchFieldException {
    T row = implClass.newInstance();
    byte[] rowKeyRaw = result.getRow();
    setField(row, keyVarName, keyType, rowKeyRaw);
    for (String f : fields) {
      Field field = fieldMap.get(f);
      Class<?> clazz = field.getType();
      HbaseColumn col = columnMap.get(f);
      if (Map.class.isAssignableFrom(clazz)) {
        NavigableMap<byte[], byte[]> qualMap =
          result.getNoVersionMap().get(col.getFamily());
        Map map = (Map) clazz.newInstance();
        ParameterizedType paramType = (ParameterizedType) field.getGenericType();
        Class<?> keyClass = (Class<?>) paramType.getActualTypeArguments()[0];
        Class<?> valueClass = (Class<?>) paramType.getActualTypeArguments()[1];
        for (Entry<byte[], byte[]> e : qualMap.entrySet()) {
          map.put(parseAsInstanceOf(keyClass, e.getKey()), 
              parseAsInstanceOf(valueClass, e.getValue()));
        }
        setField(row, field.getName(), map);
      } else {
        byte[] val =
          result.getValue(col.getFamily(), col.getQualifier());
        setField(row, field.getName(), clazz, val);
      }
    }
    return row;
  }

  private Object parseAsInstanceOf(Class<?> clazz, byte[] val) {
    if (clazz.equals(Byte.TYPE)) {
      return val[0];
    } else if (clazz.equals(Boolean.TYPE)) {
      return val[0] == 0 ? false : true;
    } else if (clazz.equals(Short.TYPE)) {
      return Bytes.toShort(val);
    } else if (clazz.equals(Integer.TYPE)) {
      return Bytes.toInt(val);
    } else if (clazz.equals(Long.TYPE)) {
      return Bytes.toLong(val);
    } else if (clazz.equals(Float.TYPE)) {
      return Bytes.toFloat(val);
    } else if (clazz.equals(Double.TYPE)) {
      return Bytes.toDouble(val);
    } else if (clazz.equals(String.class)) {
      return Bytes.toString(val);
    } else if (clazz.equals(Utf8.class)) {
      return new Utf8(Bytes.toString(val));
    }
    throw new RuntimeException("Can't parse data as class: " + clazz);
  }
  
  private byte[] toBytes(Object o) {
    Class<?> clazz = o.getClass();
    if (clazz.equals(Byte.TYPE) || clazz.equals(Byte.class)) {
      return new byte[] { (Byte) o };
    } else if (clazz.equals(Boolean.TYPE) || clazz.equals(Boolean.class)) {
      return new byte[] { ((Boolean) o ? (byte) 1 :(byte) 0)};
    } else if (clazz.equals(Short.TYPE) || clazz.equals(Short.class)) {
      return Bytes.toBytes((Short) o);
    } else if (clazz.equals(Integer.TYPE) || clazz.equals(Integer.class)) {
      return Bytes.toBytes((Integer) o);
    } else if (clazz.equals(Long.TYPE) || clazz.equals(Long.class)) {
      return Bytes.toBytes((Long) o);
    } else if (clazz.equals(Float.TYPE) || clazz.equals(Float.class)) {
      return Bytes.toBytes((Float) o);
    } else if (clazz.equals(Double.TYPE) || clazz.equals(Double.class)) {
      return Bytes.toBytes((Double) o);
    } else if (clazz.equals(String.class)) {
      return Bytes.toBytes((String) o);
    } else if (clazz.equals(Utf8.class)) {
      return ((Utf8) o).getBytes();
    }
    throw new RuntimeException("Can't parse data as class: " + clazz);
  }
  
  @SuppressWarnings("unchecked")
  private void setField(T row, String f, Map map)
  throws SecurityException, NoSuchFieldException,
  IllegalArgumentException, IllegalAccessException {
    Field field = row.getClass().getDeclaredField(f);
    field.setAccessible(true);
    field.set(row, map);    
  }

  private void setField(T row, String f, Class<?> clazz, byte[] val)
  throws SecurityException, NoSuchFieldException,
  IllegalArgumentException, IllegalAccessException {
    Field field = row.getClass().getDeclaredField(f);
    field.setAccessible(true);
    field.set(row, parseAsInstanceOf(clazz, val));
  }

  private static String getAttr(Node node, String attr) {
    NamedNodeMap nnm = node.getAttributes();
    Node itemNode = nnm.getNamedItem(attr);
    if (itemNode == null) {
      return null;
    }
    return itemNode.getNodeValue();
  }
  
  private void extractFieldTypes() {
    for (Field f : implClass.getDeclaredFields()) {
      RowKey rowKey = f.getAnnotation(RowKey.class);
      if (rowKey != null) {
        keyVarName = f.getName();
        keyType = f.getType();
        continue;
      }
      RowField nutchField = f.getAnnotation(RowField.class);
      if (nutchField == null) {
        continue;
      }
      String fieldName = nutchField.name();
      if (fieldName.equals("")) {
        fieldName = f.getName();
      }
      fieldMap.put(fieldName, f);
      annMap.put(fieldName, nutchField);
    }
  }
  
  @SuppressWarnings("unchecked")
  private void parseMapping(String fileName)
  throws ClassNotFoundException {
    try {
      Document doc = docBuilder.parse(getConf().getConfResourceAsInputStream(fileName));
      NodeWalker walker = new NodeWalker(doc.getFirstChild());
      while (walker.hasNext()) {
        Node node = walker.nextNode();
        if (node.getNodeType() == Node.TEXT_NODE) {
          continue;
        }
        if (node.getNodeName().equals("table")) {
          table = new HTable(getAttr(node, "name"));
          implClass = (Class<T>) Class.forName(getAttr(node, "class"));
          extractFieldTypes();
        } else if (node.getNodeName().equals("field")) {
          String fieldName = getAttr(node, "name");
          String familyStr = getAttr(node, "family");
          String qualifierStr = getAttr(node, "qualifier");
          byte[] family = Bytes.toBytes(familyStr);
          byte[] qualifier =
            qualifierStr != null ? Bytes.toBytes(qualifierStr) : null;
          columnMap.put(fieldName, new HbaseColumn(family, qualifier));
        }
      }
    } catch (SAXException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  public static void main(String[] args) throws Exception {
    HbaseSerializer<WebTableRow, Utf8> hs = new HbaseSerializer<WebTableRow, Utf8>(NutchConfiguration.create());
    WebTableRow row = hs.readRow("http://com.google/", new String[] { "fetchTime", "title", "text", "status" });
    System.out.println(row.getFetchTime());
    System.out.println(row.getRowKey());
    System.out.println(row.getText());
    System.out.println(row.getTitle());
    System.out.println(row.getStatus());
    //row.setRowKey(new Utf8("baska"));
    //row.setStatus(row.getStatus() + 1);
    //row.setTitle(new Utf8("Title updated!"));
    //hs.writeRow(row);
  }
}
