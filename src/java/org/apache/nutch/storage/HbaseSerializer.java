package org.apache.nutch.storage;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.FactoryConfigurationError;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.util.Utf8;
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

public class HbaseSerializer<K, R extends NutchTableRow>
implements NutchSerializer<K, R> {
  
  public static final String DEFAULT_FILE_NAME = "hbase-mapping.xml";
  
  private static final DocumentBuilder docBuilder; 

  // a map from field name to hbase column
  private Map<String, HbaseColumn> columnMap;
  
  private HTable table;
  
  private Class<R> implClass;
  
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

  public HbaseSerializer()  {
    columnMap = new HashMap<String, HbaseColumn>();
    try {
      parseMapping(DEFAULT_FILE_NAME);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public R readRow(K key, String[] fields) throws IOException {
    try {
      Get get = new Get(toBytes(key));
      R row = implClass.newInstance();
      Schema schema = row.getSchema();
      Map<String, Field> fieldMap = schema.getFields();
      for (String f : fields) {
        HbaseColumn col = columnMap.get(f);
        Schema fieldSchema = fieldMap.get(f).schema();
        if (fieldSchema.getType() == Type.MAP) {
          get.addFamily(col.family);
        } else {
          get.addColumn(col.family, col.qualifier);
        }
      }
      Result result = table.get(get);
      return makeNutchTableRow(row, result, fields);
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
  public void writeRow(K key, R row) throws IOException {
    Schema schema = row.getSchema();
    int i = 0;
    Put put = new Put(toBytes(key));
    for (Map.Entry<String, Schema> field : schema.getFieldSchemas()) {
      if (!row.isFieldChanged(i)) {
        i++;
        continue;
      }
      Object o = row.get(i);
      HbaseColumn hcol = columnMap.get(field.getKey());
      put.add(hcol.getFamily(), hcol.getQualifier(), toBytes(o));
      i++;
    }
    table.put(put);
    table.flushCommits();
  }

  private R makeNutchTableRow(R row, Result result, String[] fields)
  throws InstantiationException, IllegalAccessException, SecurityException, NoSuchFieldException {
    Schema schema = row.getSchema();
    Map<String, Field> fieldMap = schema.getFields();
    for (String f : fields) {
      HbaseColumn col = columnMap.get(f);
      /*if (Map.class.isAssignableFrom(clazz)) {
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
      } else {*/
      byte[] val =
         result.getValue(col.getFamily(), col.getQualifier());
      setField(row, fieldMap.get(f), val);
      //}
    }
    return row;
  }

  private Object parseAsInstanceOf(Type type, byte[] val) {
    switch (type) {
    case STRING:  return new Utf8(Bytes.toString(val));
    case BYTES:   return val;
    case INT:     return Bytes.toInt(val);
    case LONG:    return Bytes.toLong(val);
    case FLOAT:   return Bytes.toFloat(val);
    case DOUBLE:  return Bytes.toDouble(val);
    case BOOLEAN: return val[0] != 0;
    default: throw new RuntimeException("Unknown type: "+type);
    }
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
  
  /*private void setField(R row, String f, Map map)
  throws SecurityException, NoSuchFieldException,
  IllegalArgumentException, IllegalAccessException {
    Field field = row.getClass().getDeclaredField(f);
    field.setAccessible(true);
    field.set(row, map);    
  }*/

  private void setField(R row, Field field, byte[] val) {
    row.set(field.pos(), parseAsInstanceOf(field.schema().getType(), val));
  }

  private static String getAttr(Node node, String attr) {
    NamedNodeMap nnm = node.getAttributes();
    Node itemNode = nnm.getNamedItem(attr);
    if (itemNode == null) {
      return null;
    }
    return itemNode.getNodeValue();
  }
  
  @SuppressWarnings("unchecked")
  private void parseMapping(String fileName)
  throws ClassNotFoundException {
    try {
      InputStream stream =
        HbaseSerializer.class.getClassLoader().getResourceAsStream(fileName);
      Document doc = docBuilder.parse(stream);
      NodeWalker walker = new NodeWalker(doc.getFirstChild());
      while (walker.hasNext()) {
        Node node = walker.nextNode();
        if (node.getNodeType() == Node.TEXT_NODE) {
          continue;
        }
        if (node.getNodeName().equals("table")) {
          table = new HTable(getAttr(node, "name"));
          implClass = (Class<R>) Class.forName(getAttr(node, "class"));
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
    NutchSerializer<String, WebTableRow> hs =
      NutchSerializerFactory.create(NutchConfiguration.create());
    WebTableRow row = hs.readRow("http://com.google/", new String[] { "fetchTime", "title", "text", "status" });
    System.out.println(row.getFetchTime());
    System.out.println(row.getText());
    System.out.println(row.getTitle());
    System.out.println(row.getStatus());
    //row.setRowKey(new Utf8("baska"));
    //row.setStatus(row.getStatus() + 1);
    //row.setTitle(new Utf8("Title updated!"));
    //hs.writeRow(row);
  }
}
