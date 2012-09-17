package org.apache.solr.common.cloud;

/*
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

import org.apache.noggit.JSONUtil;
import org.apache.noggit.JSONWriter;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * ZkNodeProps contains generic immutable properties.
 */
public class ZkNodeProps implements JSONWriter.Writable {

  protected final Map<String,Object> propMap;

  /**
   * Construct ZKNodeProps from map.
   */
  public ZkNodeProps(Map<String,Object> propMap) {
    this.propMap = propMap;
  }


  /**
   * Constructor that populates the from array of Strings in form key1, value1,
   * key2, value2, ..., keyN, valueN
   */
  public ZkNodeProps(String... keyVals) {
    this( makeMap((Object[])keyVals) );
  }

  public static ZkNodeProps fromKeyVals(Object... keyVals)  {
    return new ZkNodeProps( makeMap(keyVals) );
  }

  public static Map<String,Object> makeMap(Object... keyVals) {
    if ((keyVals.length & 0x01) != 0) {
      throw new IllegalArgumentException("arguments should be key,value");
    }
    Map<String,Object> propMap = new HashMap<String,Object>(keyVals.length>>1);
    for (int i = 0; i < keyVals.length; i+=2) {
      propMap.put(keyVals[i].toString(), keyVals[i+1]);
    }
    return propMap;
  }


  /**
   * Get property keys.
   */
  public Set<String> keySet() {
    return Collections.unmodifiableSet(propMap.keySet());
  }

  /**
   * Get all properties as map.
   */
  public Map<String, Object> getProperties() {
    return Collections.unmodifiableMap(propMap);
  }

  /** Returns a shallow writable copy of the properties */
  public Map<String,Object> shallowCopy() {
    return new LinkedHashMap<String, Object>(propMap);
  }

  /**
   * Create Replica from json string that is typically stored in zookeeper.
   */
  public static ZkNodeProps load(byte[] bytes) {
    Map<String, Object> props = (Map<String, Object>) ZkStateReader.fromJSON(bytes);
    return new ZkNodeProps(props);
  }

  @Override
  public void write(JSONWriter jsonWriter) {
    jsonWriter.write(propMap);
  }
  
  /**
   * Get a string property value.
   */
  public String getStr(String key) {
    Object o = propMap.get(key);
    return o == null ? null : o.toString();
  }

  public Object get(String key) {
    return propMap.get(key);
  }
  
  @Override
  public String toString() {
    return JSONUtil.toJSON(this);
    /***
    StringBuilder sb = new StringBuilder();
    Set<Entry<String,Object>> entries = propMap.entrySet();
    for(Entry<String,Object> entry : entries) {
      sb.append(entry.getKey() + "=" + entry.getValue() + "\n");
    }
    return sb.toString();
    ***/
  }

  /**
   * Check if property key exists.
   */
  public boolean containsKey(String key) {
    return propMap.containsKey(key);
  }

}
