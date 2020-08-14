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

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.MapWriter;

/**
 *  A simple abstraction of a record containing key/value pairs.
 *  Convenience methods are provided for returning single and multiValue String, Long and Double values.
 *  Note that ints and floats are treated as longs and doubles respectively.
 *
**/

public class Tuple implements Cloneable, MapWriter {

  /**
   *  When EOF field is true the Tuple marks the end of the stream.
   *  The EOF Tuple will not contain a record from the stream, but it may contain
   *  metrics/aggregates gathered by underlying streams.
   * */

  public boolean EOF;
  public boolean EXCEPTION;

  public Map fields = new HashMap();
  public List<String> fieldNames;
  public Map<String, String> fieldLabels;

  public Tuple(){
    // just an empty tuple
  }
  
  public Tuple(Map fields) {
    if(fields.containsKey("EOF")) {
      EOF = true;
    }

    if(fields.containsKey("EXCEPTION")){
      EXCEPTION = true;
    }

    this.fields.putAll(fields);
  }

  public Object get(Object key) {
    return this.fields.get(key);
  }

  public void put(Object key, Object value) {
    this.fields.put(key, value);
  }
  
  public void remove(Object key){
    this.fields.remove(key);
  }

  public String getString(Object key) {
    return String.valueOf(this.fields.get(key));
  }

  public String getException(){ return (String)this.fields.get("EXCEPTION"); }

  public Long getLong(Object key) {
    Object o = this.fields.get(key);

    if(o == null) {
      return null;
    }

    if(o instanceof Long) {
      return (Long) o;
    } else if (o instanceof Number) {
      return ((Number)o).longValue();
    } else {
      //Attempt to parse the long
      return Long.parseLong(o.toString());
    }
  }

  // Convenience method since Booleans can be passed around as Strings.
  public Boolean getBool(Object key) {
    Object o = this.fields.get(key);

    if (o == null) {
      return null;
    }

    if (o instanceof Boolean) {
      return (Boolean) o;
    } else {
      //Attempt to parse the Boolean
      return Boolean.parseBoolean(o.toString());
    }
  }

  public List<Boolean> getBools(Object key) {
    return (List<Boolean>) this.fields.get(key);
  }

  // Convenience methods since the dates are actually shipped around as Strings.
  public Date getDate(Object key) {
    Object o = this.fields.get(key);

    if (o == null) {
      return null;
    }

    if (o instanceof Date) {
      return (Date) o;
    } else {
      //Attempt to parse the Date from a String
      return new Date(Instant.parse(o.toString()).toEpochMilli());
    }
  }

  public List<Date> getDates(Object key) {
    List<String> vals = (List<String>) this.fields.get(key);
    if (vals == null) return null;
    
    List<Date> ret = new ArrayList<>();
    for (String dateStr : (List<String>) this.fields.get(key)) {
      ret.add(new Date(Instant.parse(dateStr).toEpochMilli()));
    }
    return ret;
  }

  public Double getDouble(Object key) {
    Object o = this.fields.get(key);

    if(o == null) {
      return null;
    }

    if(o instanceof Double) {
      return (Double)o;
    } else {
      //Attempt to parse the double
      return Double.parseDouble(o.toString());
    }
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

  public Map getMap() {
    return this.fields;
  }

  public List<Map> getMaps(Object key) {
    return (List<Map>)this.fields.get(key);
  }

  public void setMaps(Object key, List<Map> maps) {
    this.fields.put(key, maps);
  }

  public Map<String,Map> getMetrics() {
    return (Map<String,Map>)this.fields.get("_METRICS_");
  }

  public void setMetrics(Map<String, Map> metrics) {
    this.fields.put("_METRICS_", metrics);
  }

  public Tuple clone() {
    HashMap m = new HashMap(fields);
    Tuple clone = new Tuple(m);
    return clone;
  }
  
  public void merge(Tuple other){
    fields.putAll(other.getMap());
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    if(fieldNames == null) {
      fields.forEach((k, v) -> {
        try {
          ew.put((String) k, v);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    } else {
      for(String fieldName : fieldNames) {
        String label = fieldLabels.get(fieldName);
        ew.put(label, fields.get(label));
      }
    }
  }
}
