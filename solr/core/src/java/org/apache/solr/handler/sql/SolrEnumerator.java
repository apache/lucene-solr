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
package org.apache.solr.handler.sql;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

/** Enumerator that reads from a Solr collection. */
class SolrEnumerator implements Enumerator<Object> {
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final TupleStream tupleStream;
  private final List<Map.Entry<String, Class>> fields;
  private Tuple current;
  private char sep = 31;

  /** Creates a SolrEnumerator.
   *
   * @param tupleStream Solr TupleStream
   * @param fields Fields to get from each Tuple
   */
  SolrEnumerator(TupleStream tupleStream, List<Map.Entry<String, Class>> fields) {
    this.tupleStream = tupleStream;
    try {
      this.tupleStream.open();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    this.fields = fields;
    this.current = null;
  }

  /** Produce the next row from the results
   *
   * @return A new row from the results
   */
  public Object current() {
    if (fields.size() == 1) {
      return this.getter(current, fields.get(0));
    } else {
      // Build an array with all fields in this row
      Object[] row = new Object[fields.size()];
      for (int i = 0; i < fields.size(); i++) {
        row[i] = this.getter(current, fields.get(i));
      }

      return row;
    }
  }

  private Object getter(Tuple tuple, Map.Entry<String, Class> field) {
    Object val = tuple.get(field.getKey());

    if(val == null) {
      return null;
    }

    Class clazz = field.getValue();
    if(clazz.equals(Long.class)) {
      if(val instanceof Double) {
        return this.getRealVal(val);
      }
      return val;
    }

    if(val instanceof ArrayList) {
      ArrayList arrayList = (ArrayList) val;
      StringBuilder buf = new StringBuilder();

      for(Object o : arrayList) {
        buf.append(sep);
        buf.append(o.toString());
      }
      val = buf.toString();
    }

    return val;
  }

  private Object getRealVal(Object val) {
    // Check if Double is really a Long
    if(val instanceof Double) {
      double doubleVal = (double) val;
      //make sure that double has no decimals and fits within Long
      if(doubleVal % 1 == 0 && doubleVal >= Long.MIN_VALUE && doubleVal <= Long.MAX_VALUE) {
        return (long)doubleVal;
      }
      return doubleVal;
    }

    // Wasn't a double so just return original Object
    return val;
  }

  public boolean moveNext() {
    try {
      Tuple tuple = this.tupleStream.read();
      if (tuple.EOF) {
        return false;
      } else {
        current = tuple;
        return true;
      }
    } catch (IOException e) {
      logger.error("IOException", e);
      return false;
    }
  }

  public void reset() {
    throw new UnsupportedOperationException();
  }

  public void close() {
    if(this.tupleStream != null) {
      try {
        this.tupleStream.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
