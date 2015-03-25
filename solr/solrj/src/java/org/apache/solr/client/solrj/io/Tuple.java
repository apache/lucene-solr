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

package org.apache.solr.client.solrj.io;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Iterator;


/**
 *  A simple abstraction of a record containing key/value pairs.
 *  Convenience methods are provided for returning single and multiValue String, Long and Double values.
 *  Note that ints and floats are treated as longs and doubles respectively.
 *
**/

public class Tuple implements Cloneable {

  public boolean EOF;
  public Map fields = new HashMap();

  public Tuple(Map fields) {
    if(fields.containsKey("EOF")) {
      EOF = true;
    }

    this.fields.putAll(fields);
  }

  public Object get(Object key) {
    return this.fields.get(key);
  }

  public void put(Object key, Object value) {
    this.fields.put(key, value);
  }

  public String getString(Object key) {
    return (String)this.fields.get(key);
  }

  public Long getLong(Object key) {
    return (Long)this.fields.get(key);
  }

  public Double getDouble(Object key) {
    return (Double)this.fields.get(key);
  }

  public List<String> getStrings(Object key) {
    return (List<String>)this.fields.get(key);
  }

  public List<Long> getLongs(Object key) {
    return (List<Long>)this.fields.get(key);
  }

  public List<Double> getDoubles(Object key) {
    return (List<Double>)this.fields.get(key);
  }

  public Iterator<Map.Entry> getFields() {
    return fields.entrySet().iterator();
  }

  public Map getMap() {
    return this.fields;
  }

  public List<Map> getMaps() {
    return (List<Map>)this.fields.get("_MAPS_");
  }

  public void setMaps(List<Map> maps) {
    this.fields.put("_MAPS_", maps);

  }


  public Tuple clone() {
    HashMap m = new HashMap();
    m.putAll(fields);
    Tuple clone = new Tuple(m);
    return clone;
  }
}