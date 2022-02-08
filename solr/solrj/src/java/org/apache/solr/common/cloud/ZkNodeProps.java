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
package org.apache.solr.common.cloud;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.Utils;
import org.noggit.JSONWriter;

import static org.apache.solr.common.util.Utils.toJSONString;

/**
 * ZkNodeProps contains generic immutable properties.
 */
public class ZkNodeProps implements JSONWriter.Writable {

  /**
   * Deprecated: Do not use, will be removed in 9.0.
   * Was a feature flag to enable storing the 'base_url' property; base_url will not be stored as of Solr 9.x.
   * Installations that use an older (pre-8.8) SolrJ against a 8.8.0 or newer server will need to set this system
   * property to true to avoid NPEs when reading cluster state from Zookeeper, see SOLR-15145.
   * @deprecated to be removed in Solr 9.0 (see: SOLR-15587)
   */
  @Deprecated
  public static final boolean STORE_BASE_URL = true;

  protected final Map<String,Object> propMap;

  /**
   * Construct ZKNodeProps from map.
   */
  public ZkNodeProps(Map<String,Object> propMap) {
    this.propMap = propMap;

    // TODO: store an unmodifiable map, but in a way that guarantees not to wrap more than once.
    // Always wrapping introduces a memory leak.
  }

  public ZkNodeProps plus(String key , Object val) {
    return plus(Collections.singletonMap(key,val));
  }

  public ZkNodeProps plus(Map<String, Object> newVals) {
    LinkedHashMap<String, Object> copy = new LinkedHashMap<>(propMap);
    if (newVals == null || newVals.isEmpty()) return new ZkNodeProps(copy);
    copy.putAll(newVals);
    return new ZkNodeProps(copy);
  }


  /**
   * Constructor that populates the from array of Strings in form key1, value1,
   * key2, value2, ..., keyN, valueN
   */
  public ZkNodeProps(String... keyVals) {
    this( Utils.makeMap((Object[]) keyVals) );
  }

  public static ZkNodeProps fromKeyVals(Object... keyVals)  {
    return new ZkNodeProps( Utils.makeMap(keyVals) );
  }


  /**
   * Get property keys.
   */
  public Set<String> keySet() {
    return propMap.keySet();
  }

  /**
   * Get all properties as map.
   */
  public Map<String, Object> getProperties() {
    return propMap;
  }

  /** Returns a shallow writable copy of the properties */
  public Map<String,Object> shallowCopy() {
    return new LinkedHashMap<>(propMap);
  }

  /**
   * Create Replica from json string that is typically stored in zookeeper.
   */
  @SuppressWarnings({"unchecked"})
  public static ZkNodeProps load(byte[] bytes) {
    Map<String, Object> props;
    if (bytes[0] == 2) {
      try (JavaBinCodec jbc = new JavaBinCodec()) {
        props = (Map<String, Object>) jbc.unmarshal(bytes);
      } catch (IOException e) {
        throw new RuntimeException("Unable to parse javabin content");
      }
    } else {
      props = (Map<String, Object>) Utils.fromJSON(bytes);
    }
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
    return getStr(key, null);
  }

  /**
   * Get a string property value.
   */
  public Integer getInt(String key, Integer def) {
    Object o = propMap.get(key);
    return o == null ? def : Integer.valueOf(o.toString());
  }

  /**
   * Get a string property value.
   */
  public String getStr(String key, String def) {
    Object o = propMap.get(key);
    return o == null ? def : o.toString();
  }

  public Object get(String key) {
    return propMap.get(key);
  }

  @Override
  public String toString() {
    return toJSONString(this);
  }

  /**
   * Check if property key exists.
   */
  public boolean containsKey(String key) {
    return propMap.containsKey(key);
  }
  
  public boolean getBool(String key, boolean b) {
    Object o = propMap.get(key);
    if (o == null) return b;
    if (o instanceof Boolean) return (boolean) o;
    return Boolean.parseBoolean(o.toString());
  }

  @Override
  public boolean equals(Object that) {
    return that instanceof ZkNodeProps && ((ZkNodeProps)that).propMap.equals(this.propMap);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(propMap);
  }
}
