package org.apache.solr.search.function;
/**
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

import org.apache.solr.search.SolrIndexReader;
import org.apache.lucene.index.IndexReader;

import java.io.IOException;
import java.util.Map;

/**
 * A value source that wraps another and ensures that the top level reader
 * is used.  This is useful for value sources like ord() who's value depend
 * on all those around it.
 */
public class TopValueSource extends ValueSource {
  private final ValueSource vs;

  public TopValueSource(ValueSource vs) {
    this.vs = vs;
  }

  public ValueSource getValueSource() {
    return vs;
  }

  public String description() {
    return "top(" + vs.description() + ')';
  }

  public DocValues getValues(Map context, IndexReader reader) throws IOException {
    int offset = 0;
    IndexReader topReader = reader;
    if (topReader instanceof SolrIndexReader) {
      SolrIndexReader r = (SolrIndexReader)topReader;
      while (r.getParent() != null) {
        offset += r.getBase();
        r = r.getParent();
      }
      topReader = r;
    }
    final int off = offset;
    final DocValues vals = vs.getValues(context, topReader);
    if (topReader == reader) return vals;

    return new DocValues() {
      public float floatVal(int doc) {
        return vals.floatVal(doc + off);
      }

      public int intVal(int doc) {
        return vals.intVal(doc + off);
      }

      public long longVal(int doc) {
        return vals.longVal(doc + off);
      }

      public double doubleVal(int doc) {
        return vals.doubleVal(doc + off);
      }

      public String strVal(int doc) {
        return vals.strVal(doc + off);
      }

      public String toString(int doc) {
        return vals.strVal(doc + off);
      }
    };
  }

  public boolean equals(Object o) {
    if (o.getClass() !=  TopValueSource.class) return false;
    TopValueSource other = (TopValueSource)o;
    return vs.equals(other.vs);
  }

  public int hashCode() {
    int h = vs.hashCode();
    return (h<<1) | (h>>>31);
  }

  public String toString() {
    return "top("+vs.toString()+')';
  }
}