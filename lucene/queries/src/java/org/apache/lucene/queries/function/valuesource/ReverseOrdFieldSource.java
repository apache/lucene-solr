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

package org.apache.lucene.queries.function.valuesource;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.CompositeReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.IntDocValues;
import org.apache.lucene.search.FieldCache;

/**
 * Obtains the ordinal of the field value from the default Lucene {@link org.apache.lucene.search.FieldCache} using getTermsIndex()
 * and reverses the order.
 * <br>
 * The native lucene index order is used to assign an ordinal value for each field value.
 * <br>Field values (terms) are lexicographically ordered by unicode value, and numbered starting at 1.
 * <br>
 * Example of reverse ordinal (rord):<br>
 *  If there were only three field values: "apple","banana","pear"
 * <br>then rord("apple")=3, rord("banana")=2, ord("pear")=1
 * <p>
 *  WARNING: ord() depends on the position in an index and can thus change when other documents are inserted or deleted,
 *  or if a MultiSearcher is used.
 * <br>
 *  WARNING: as of Solr 1.4, ord() and rord() can cause excess memory use since they must use a FieldCache entry
 * at the top level reader, while sorting and function queries now use entries at the segment level.  Hence sorting
 * or using a different function query, in addition to ord()/rord() will double memory use.
 * 
 *
 */

public class ReverseOrdFieldSource extends ValueSource {
  public final String field;

  public ReverseOrdFieldSource(String field) {
    this.field = field;
  }

  @Override
  public String description() {
    return "rord("+field+')';
  }

  // TODO: this is trappy? perhaps this query instead should make you pass a slow reader yourself?
  @Override
  public FunctionValues getValues(Map context, AtomicReaderContext readerContext) throws IOException {
    final IndexReader topReader = ReaderUtil.getTopLevelContext(readerContext).reader();
    final AtomicReader r = topReader instanceof CompositeReader 
        ? new SlowCompositeReaderWrapper((CompositeReader)topReader) 
        : (AtomicReader) topReader;
    final int off = readerContext.docBase;

    final SortedDocValues sindex = FieldCache.DEFAULT.getTermsIndex(r, field);
    final int end = sindex.getValueCount();

    return new IntDocValues(this) {
     @Override
      public int intVal(int doc) {
        return (end - sindex.getOrd(doc+off) - 1);
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || (o.getClass() !=  ReverseOrdFieldSource.class)) return false;
    ReverseOrdFieldSource other = (ReverseOrdFieldSource)o;
    return this.field.equals(other.field);
  }

  private static final int hcode = ReverseOrdFieldSource.class.hashCode();
  @Override
  public int hashCode() {
    return hcode + field.hashCode();
  }

}
