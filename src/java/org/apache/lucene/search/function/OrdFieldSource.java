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

package org.apache.lucene.search.function;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.FieldCache;

import java.io.IOException;

/**
 * Expert: obtains the ordinal of the field value from the default Lucene 
 * {@link org.apache.lucene.search.FieldCache Fieldcache} using getStringIndex().
 * <p>
 * The native lucene index order is used to assign an ordinal value for each field value.
 * <p
 * Field values (terms) are lexicographically ordered by unicode value, and numbered starting at 1.
 * <p>
 * Example:
 * <br>If there were only three field values: "apple","banana","pear"
 * <br>then ord("apple")=1, ord("banana")=2, ord("pear")=3
 * <p>
 * WARNING: 
 * ord() depends on the position in an index and can thus change 
 * when other documents are inserted or deleted,
 * or if a MultiSearcher is used. 
 *
 * <p><font color="#FF0000">
 * WARNING: The status of the <b>search.function</b> package is experimental. 
 * The APIs introduced here might change in the future and will not be 
 * supported anymore in such a case.</font>
 *
 * <p><b>NOTE</b>: with the switch in 2.9 to segment-based
 * searching, if {@link #getValues} is invoked with a
 * composite (multi-segment) reader, this can easily cause
 * double RAM usage for the values in the FieldCache.  It's
 * best to switch your application to pass only atomic
 * (single segment) readers to this API.  Alternatively, for
 * a short-term fix, you could wrap your ValueSource using
 * {@link MultiValueSource}, which costs more CPU per lookup
 * but will not consume double the FieldCache RAM.</p>
 */

public class OrdFieldSource extends ValueSource {
  protected String field;

  /** 
   * Constructor for a certain field.
   * @param field field whose values order is used.  
   */
  public OrdFieldSource(String field) {
    this.field = field;
  }

  /*(non-Javadoc) @see org.apache.lucene.search.function.ValueSource#description() */
  public String description() {
    return "ord(" + field + ')';
  }

  /*(non-Javadoc) @see org.apache.lucene.search.function.ValueSource#getValues(org.apache.lucene.index.IndexReader) */
  public DocValues getValues(IndexReader reader) throws IOException {
    final int[] arr = FieldCache.DEFAULT.getStringIndex(reader, field).order;
    return new DocValues() {
      /*(non-Javadoc) @see org.apache.lucene.search.function.DocValues#floatVal(int) */
      public float floatVal(int doc) {
        return (float)arr[doc];
      }
      /*(non-Javadoc) @see org.apache.lucene.search.function.DocValues#strVal(int) */
      public String strVal(int doc) {
        // the string value of the ordinal, not the string itself
        return Integer.toString(arr[doc]);
      }
      /*(non-Javadoc) @see org.apache.lucene.search.function.DocValues#toString(int) */
      public String toString(int doc) {
        return description() + '=' + intVal(doc);
      }
      /*(non-Javadoc) @see org.apache.lucene.search.function.DocValues#getInnerArray() */
      Object getInnerArray() {
        return arr;
      }
    };
  }

  /*(non-Javadoc) @see java.lang.Object#equals(java.lang.Object) */
  public boolean equals(Object o) {
    if (o.getClass() !=  OrdFieldSource.class) return false;
    OrdFieldSource other = (OrdFieldSource)o;
    return this.field.equals(other.field);
  }

  private static final int hcode = OrdFieldSource.class.hashCode();
  
  /*(non-Javadoc) @see java.lang.Object#hashCode() */
  public int hashCode() {
    return hcode + field.hashCode();
  }
}
