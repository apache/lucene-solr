package org.apache.solr.cloud;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Set;
import java.util.Map.Entry;

public class ZkNodeProps extends HashMap<String,String> {

  private static final long serialVersionUID = 1L;

  public void load(InputStream in) throws IOException {
    DataInputStream dis = new DataInputStream(in);
    String stringRep = null;
    try {
      stringRep = dis.readUTF();
    } finally {
      dis.close();
    }
    String[] lines = stringRep.split("\n");
    for (String line : lines) {
      int sepIndex = line.indexOf('=');
      String key = line.substring(0, sepIndex);
      String value = line.substring(sepIndex + 1, line.length());
      put(key, value);
    }
  }

  public void store(OutputStream out) throws IOException {
    StringBuilder sb = new StringBuilder();
    Set<Entry<String,String>> entries = entrySet();
    for(Entry<String,String> entry : entries) {
      sb.append(entry.getKey() + "=" + entry.getValue() + "\n");
    }
    DataOutputStream dos = new DataOutputStream(out);
    try {
      dos.writeUTF(sb.toString());
    } finally {
      dos.close();
    }
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
