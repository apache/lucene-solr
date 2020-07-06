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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.MapWriter;
import org.apache.solr.common.params.StreamParams;

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
  /**
   * When EXCEPTION field is true the Tuple marks an exception in the stream
   * and the corresponding "EXCEPTION" field contains a related message.
   */
  public boolean EXCEPTION;

  /**
   * Tuple fields.
   * @deprecated use {@link #getFields()} instead of this public field.
   */
  @Deprecated
  public Map<Object, Object> fields = new HashMap<>(2);
  /**
   * External serializable field names.
   * @deprecated use {@link #getFieldNames()} instead of this public field.
   */
  @Deprecated
  public List<String> fieldNames;
  /**
   * Mapping of external field names to internal tuple field names.
   * @deprecated use {@link #getFieldLabels()} instead of this public field.
   */
  @Deprecated
  public Map<String, String> fieldLabels;

  public Tuple() {
    // just an empty tuple
  }

  /**
   * A copy constructor.
   * @param fields map containing keys and values to be copied to this tuple
   */
  public Tuple(Map<?, ?> fields) {
    for (Map.Entry<?, ?> entry : fields.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Constructor that accepts an even number of arguments as key / value pairs.
   * @param fields a list of key / value pairs, with keys at odd and values at
   *               even positions.
   */
  public Tuple(Object... fields) {
    if (fields == null) {
      return;
    }
    if ((fields.length % 2) != 0) {
      throw new RuntimeException("must have a matching number of key-value pairs");
    }
    for (int i = 0; i < fields.length; i += 2) {
      // skip empty entries
      if (fields[i] == null) {
        continue;
      }
      put(fields[i], fields[i + 1]);
    }
  }

  public Object get(Object key) {
    return this.fields.get(key);
  }

  public void put(Object key, Object value) {
    this.fields.put(key, value);
    if (key.equals(StreamParams.EOF)) {
      EOF = true;
    } else if (key.equals(StreamParams.EXCEPTION)) {
      EXCEPTION = true;
    }
  }

  public void remove(Object key) {
    this.fields.remove(key);
  }

  public String getString(Object key) {
    return String.valueOf(this.fields.get(key));
  }

  public String getException() { return (String)this.fields.get(StreamParams.EXCEPTION); }

  public Long getLong(Object key) {
    Object o = this.fields.get(key);

    if (o == null) {
      return null;
    }

    if (o instanceof Long) {
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

  @SuppressWarnings({"unchecked"})
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

  @SuppressWarnings({"unchecked"})
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

    if (o == null) {
      return null;
    }

    if (o instanceof Double) {
      return (Double)o;
    } else {
      //Attempt to parse the double
      return Double.parseDouble(o.toString());
    }
  }

  @SuppressWarnings({"unchecked"})
  public List<String> getStrings(Object key) {
    return (List<String>)this.fields.get(key);
  }

  @SuppressWarnings({"unchecked"})
  public List<Long> getLongs(Object key) {
    return (List<Long>)this.fields.get(key);
  }

  @SuppressWarnings({"unchecked"})
  public List<Double> getDoubles(Object key) {
    return (List<Double>)this.fields.get(key);
  }

  /**
   * Return all tuple fields and their values.
   */
  public Map<Object, Object> getFields() {
    return this.fields;
  }

  /**
   * Return all tuple fields.
   * @deprecated use {@link #getFields()} instead.
   */
  @Deprecated
  @SuppressWarnings({"rawtypes"})
  public Map getMap() {
    return this.fields;
  }

  /**
   * This represents the mapping of external field labels to the tuple's
   * internal field names if they are different from field names.
   * @return field labels or null
   */
  public Map<String, String> getFieldLabels() {
    return fieldLabels;
  }

  public void setFieldLabels(Map<String, String> fieldLabels) {
    this.fieldLabels = fieldLabels;
  }

  /**
   * A list of field names to serialize. This list (together with
   * the mapping in {@link #getFieldLabels()} determines what tuple values
   * are serialized and their external (serialized) names.
   * @return list of external field names or null
   */
  public List<String> getFieldNames() {
    return fieldNames;
  }

  public void setFieldNames(List<String> fieldNames) {
    this.fieldNames = fieldNames;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public List<Map> getMaps(Object key) {
    return (List<Map>) this.fields.get(key);
  }

  public void setMaps(Object key, @SuppressWarnings({"rawtypes"})List<Map> maps) {
    this.fields.put(key, maps);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public Map<String, Map> getMetrics() {
    return (Map<String, Map>) this.fields.get(StreamParams.METRICS);
  }

  @SuppressWarnings({"rawtypes"})
  public void setMetrics(Map<String, Map> metrics) {
    this.fields.put(StreamParams.METRICS, metrics);
  }

  public Tuple clone() {
    Tuple clone = new Tuple();
    clone.fields.putAll(fields);
    return clone;
  }
  
  public void merge(Tuple other) {
    fields.putAll(other.getFields());
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    if (fieldNames == null) {
      fields.forEach((k, v) -> {
        try {
          ew.put((String) k, v);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    } else {
      for (String fieldName : fieldNames) {
        String label = fieldLabels.get(fieldName);
        ew.put(label, fields.get(label));
      }
    }
  }

  /**
   * Create a new empty tuple marked as EOF.
   */
  public static Tuple EOF() {
    Tuple tuple = new Tuple();
    tuple.put(StreamParams.EOF, true);
    return tuple;
  }

  /**
   * Create a new empty tuple marked as EXCEPTION, and optionally EOF.
   * @param msg exception message
   * @param eof if true the tuple will be marked as EOF
   */
  public static Tuple EXCEPTION(String msg, boolean eof) {
    Tuple tuple = new Tuple();
    tuple.put(StreamParams.EXCEPTION, msg);
    if (eof) {
      tuple.put(StreamParams.EOF, true);
    }
    return tuple;
  }

  /**
   * Create a new empty tuple marked as EXCEPTION and optionally EOF.
   * @param t exception - full stack trace will be used as an exception message
   * @param eof if true the tuple will be marked as EOF
   */
  public static Tuple EXCEPTION(Throwable t, boolean eof) {
    StringWriter sw = new StringWriter();
    t.printStackTrace(new PrintWriter(sw));
    return EXCEPTION(sw.toString(), eof);
  }
}
