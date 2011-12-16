package org.apache.solr.common.cloud;

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

import org.apache.noggit.JSONWriter;

import java.io.IOException;
import java.util.*;

public class CoreAssignment implements JSONWriter.Writable {

  private static String CORE="core";

  private final Map<String, String> properties;

  public Map<String,String> getProperties() {
    return properties;
  }

  private CoreAssignment(Map<String, String> props) {
    this.properties = Collections.unmodifiableMap(props);
  }
  
  public CoreAssignment(String coreName, Map<String,String> properties) {
    HashMap<String,String> props = new HashMap<String,String>();
    props.putAll(properties);
    props.put(CORE, coreName);
    this.properties = Collections.unmodifiableMap(props);
  }

  public String getCoreName() {
    return properties.get(CORE);
  }

  @Override
  public void write(JSONWriter jsonWriter) {
    jsonWriter.write(properties);
  }
  
  public static CoreAssignment[] load(byte[] bytes) throws IOException {
    List<Map<String, String>> stateMaps = (List<Map<String, String>>) ZkStateReader.fromJSON(bytes);

    CoreAssignment[] states = new CoreAssignment[stateMaps.size()];
    int i = 0;
    for (Map<String,String> state : stateMaps) {
      states[i++] = new CoreAssignment(state);
    }
    
    return states;
  }
  
  @Override
  public int hashCode() {
    return properties.hashCode();
  }
  
  @Override
  public boolean equals(Object other) {
    if(other instanceof CoreAssignment) {
      CoreAssignment otherAssignment = (CoreAssignment) other;
      return this.getProperties().equals(otherAssignment.getProperties());
    }
    return false;
  }
  
  @Override
  public String toString() {
    return "Core:" + getCoreName() + " props:" + properties;
  }

}
