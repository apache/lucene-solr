package org.apache.solr.cloud;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class CoreAssignment {
  
  public String getCoreName() {
    return coreName;
  }

  public String getCollectionName() {
    return collectionName;
  }

  private final String coreName;
  private final String collectionName;
  private final Map<String, String> properties;
  
  public Map<String,String> getProperties() {
    return properties;
  }

  public CoreAssignment(String coreName, String collectionName, Map<String, String> properties) {
    this.coreName = coreName;
    this.collectionName = collectionName;
    this.properties = Collections.unmodifiableMap(properties);
  }
  
  public static byte[] tobytes(CoreAssignment... states) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    
    try {
      dos.write(states.length);
      for (CoreAssignment state : states) {
        dos.writeUTF(state.coreName);
        dos.writeUTF(state.collectionName);
        dos.write(state.properties.size());
        for(Entry<String, String> prop: state.properties.entrySet()) {
          dos.writeUTF(prop.getKey());
          if(prop.getValue()==null) {
            throw new NullPointerException("value was null for key:" + prop.getKey());
          }
          dos.writeUTF(prop.getValue());
        }
      }
      return baos.toByteArray();
    } finally {
      dos.close();
    }
  }
  
  public static CoreAssignment[] fromBytes(byte[] bytes) throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    DataInputStream dis = new DataInputStream(bais);
    try {
      int count = dis.read();
      CoreAssignment[] states = new CoreAssignment[count];
      for (int i = 0; i < count; i++) {
        String coreName = dis.readUTF();
        String collectionName = dis.readUTF();
        int propcount = dis.read();
        HashMap<String, String> props = new HashMap<String, String>();
        for(int j=0;j<propcount;j++) {
          String key = dis.readUTF();
          String value = dis.readUTF();
          props.put(key, value);
        }
        
        states[i] = new CoreAssignment(coreName, collectionName, props);
      }
      return states;
    } finally {
      dis.close();
    }
  }
  
  @Override
  public int hashCode() {
    return coreName.hashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    return hashCode() == obj.hashCode();
  }
  
  @Override
  public String toString() {
    return "coll:" + collectionName + " core:" + coreName + " props:" + properties;
  }
  
}
