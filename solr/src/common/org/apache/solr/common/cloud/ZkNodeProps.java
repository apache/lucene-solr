package org.apache.solr.common.cloud;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    Set<Entry<String,String>> entries = entrySet();
    for(Entry<String,String> entry : entries) {
      sb.append(entry.getKey() + "=" + entry.getValue() + "\n");
    }
    return sb.toString();
  }

}
