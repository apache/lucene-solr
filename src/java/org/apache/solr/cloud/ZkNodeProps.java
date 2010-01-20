package org.apache.solr.cloud;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.Map.Entry;

public class ZkNodeProps extends HashMap<String,String> {

  private static final long serialVersionUID = 1L;

  public void load(byte[] bytes) throws IOException {
    String stringRep = new String(bytes, "UTF-8");
    String[] lines = stringRep.split("\n");
    for (String line : lines) {
      int sepIndex = line.indexOf('=');
      String key = line.substring(0, sepIndex);
      String value = line.substring(sepIndex + 1, line.length());
      put(key, value);
    }
  }

  public byte[] store() throws IOException {
    StringBuilder sb = new StringBuilder();
    Set<Entry<String,String>> entries = entrySet();
    for(Entry<String,String> entry : entries) {
      sb.append(entry.getKey() + "=" + entry.getValue() + "\n");
    }
    return sb.toString().getBytes("UTF-8");
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder();
    Set<Entry<String,String>> entries = entrySet();
    for(Entry<String,String> entry : entries) {
      sb.append(entry.getKey() + "=" + entry.getValue() + "\n");
    }
    return sb.toString();
  }

}
