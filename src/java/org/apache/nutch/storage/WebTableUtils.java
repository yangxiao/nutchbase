package org.apache.nutch.storage;

import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.nutch.util.NutchConfiguration;

public class WebTableUtils {

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: WebTableUtils -create | -get <row> (-f field1,field2....)");
      System.exit(1);
    }
    NutchSerializer<String, WebTableRow> serializer =
      NutchSerializerFactory.create(NutchConfiguration.create());
    if (args[0].equals("-create")) {
      serializer.createTable();
      return;
    }
    if (args[0].equals("-get") && args.length >= 2) {
      String key = args[1];
      String[] fields = NutchFields.ALL_FIELDS;
      if (args.length > 2 && args[2].equals("-f")) {
        fields = args[3].split(",");
      }
      WebTableRow row = serializer.readRow(key, fields);
      Schema schema = row.getSchema();
      for (String field : fields) {
        if (!row.has(field)) { continue; }
        int i = 0;
        for (Entry<String, Schema> e : schema.getFieldSchemas()) {
          if (field.equals(e.getKey())) {
            break;
          }
          i++;
        }
        System.out.println(field+": " + row.get(i));
      }
    }
  }
}
