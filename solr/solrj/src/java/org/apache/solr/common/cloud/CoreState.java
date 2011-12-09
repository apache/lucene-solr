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

public class CoreState implements JSONWriter.Writable {
  private static String COLLECTION="collection";
  private static String CORE="core";

  private final Map<String, String> properties;

  private CoreState(Map<String, String> props) {
    this.properties = Collections.unmodifiableMap(props);
  }

  public CoreState(String coreName, String collectionName, Map<String,String> properties) {
    HashMap<String,String> props = new HashMap<String,String>();
    props.putAll(properties);
    props.put(COLLECTION, collectionName);
    props.put(CORE, coreName);
    this.properties = Collections.unmodifiableMap(props);
  }

  public String getCoreName() {
    return properties.get(CORE);
  }

  public String getCollectionName() {
    return properties.get(COLLECTION);
  }

  public Map<String,String> getProperties() {
    return properties;
  }

  @Override
  public void write(JSONWriter jsonWriter) {
    jsonWriter.write(properties);
  }


  public static CoreState[] load(byte[] bytes) {
    List<Map<String, String>> stateMaps = (List<Map<String, String>>) ZkStateReader.fromJSON(bytes);

    CoreState[] states = new CoreState[stateMaps.size()];
    int i = 0;
    for (Map<String,String> state : stateMaps) {
      states[i++] = new CoreState(state);
    }

    return states;
  }
  
  @Override
  public int hashCode() {
    return getCoreName().hashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    return hashCode() == obj.hashCode();
  }
  
  @Override
  public String toString() {
    return "coll:" + getCollectionName() + " core:" + getCoreName() + " props:" + properties;
  }

}
