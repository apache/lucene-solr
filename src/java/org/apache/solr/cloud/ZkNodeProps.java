package org.apache.solr.cloud;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

public class ZkNodeProps extends HashMap<String,String> {

  private static final long serialVersionUID = 1L;

  public void load(DataInputStream in) throws IOException {
    String stringRep = in.readUTF();
    String[] lines = stringRep.split("\n");
    for(String line : lines) {
      int sepIndex = line.indexOf('=');
      String key = line.substring(0, sepIndex);
      String value = line.substring(sepIndex + 1, line.length());
      put(key, value);
    }
  }
  
  public void store(DataOutputStream out) throws IOException {
    StringBuilder sb = new StringBuilder();
    Set<String> keys = keySet();
    for(String key : keys) {
      String value = get(key);
      sb.append(key + "=" + value + "\n");
    }
    out.writeUTF(sb.toString());
  }
}
