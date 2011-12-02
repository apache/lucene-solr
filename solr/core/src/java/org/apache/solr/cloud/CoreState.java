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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.noggit.CharArr;
import org.apache.noggit.JSONUtil;
import org.apache.noggit.ObjectBuilder;

public class CoreState {

  
  private static String COLLECTION="_collection";
  private static String CORE="_core";

  public String getCoreName() {
    return properties.get(CORE);
  }

  public String getCollectionName() {
    return properties.get(COLLECTION);
  }

  private final Map<String, String> properties;
  
  public Map<String,String> getProperties() {
    return properties;
  }

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
  
  public static byte[] tobytes(CoreState... states) throws IOException {
    CharArr out = new CharArr();
    out.append(JSONUtil.ARRAY_START);
    boolean first = true;
    for (CoreState state : states) {
      if (first) {
        first = false;
      } else {
        out.append(JSONUtil.VALUE_SEPARATOR);
      }
      out.append(JSONUtil.toJSON(state.properties));
      
    }
    
    out.append(JSONUtil.ARRAY_END);

    return out.toString().getBytes("utf-8");
  }
  
  public static CoreState[] fromBytes(byte[] bytes) throws IOException {
    ArrayList<CoreState> states = new ArrayList<CoreState>(); 
    List<Map<String, String>> stateMaps = (List<Map<String, String>>)ObjectBuilder.fromJSON(new String(bytes,"utf-8"));
    for (Map<String,String> state : stateMaps) {
      states.add(new CoreState(state));
    }
    
    return states.toArray(new CoreState[states.size()]);
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
