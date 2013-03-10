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
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueInt;

/**
 * Obtains the ordinal of the field value from the default Lucene {@link org.apache.lucene.search.FieldCache} using getStringIndex().
 * <br>
 * The native lucene index order is used to assign an ordinal value for each field value.
 * <br>Field values (terms) are lexicographically ordered by unicode value, and numbered starting at 1.
 * <br>
 * Example:<br>
 *  If there were only three field values: "apple","banana","pear"
 * <br>then ord("apple")=1, ord("banana")=2, ord("pear")=3
 * <p>
 * WARNING: ord() depends on the position in an index and can thus change when other documents are inserted or deleted,
 *  or if a MultiSearcher is used.
 * <br>WARNING: as of Solr 1.4, ord() and rord() can cause excess memory use since they must use a FieldCache entry
 * at the top level reader, while sorting and function queries now use entries at the segment level.  Hence sorting
 * or using a different function query, in addition to ord()/rord() will double memory use.
 *
 */

public class OrdFieldSource extends ValueSource {
  protected final String field;

  public OrdFieldSource(String field) {
    this.field = field;
  }

  @Override
  public String description() {
    return "ord(" + field + ')';
  }


  // TODO: this is trappy? perhaps this query instead should make you pass a slow reader yourself?
  @Override
  public FunctionValues getValues(Map context, AtomicReaderContext readerContext) throws IOException {
    final int off = readerContext.docBase;
    final IndexReader topReader = ReaderUtil.getTopLevelContext(readerContext).reader();
    final AtomicReader r = topReader instanceof CompositeReader 
        ? new SlowCompositeReaderWrapper((CompositeReader)topReader) 
        : (AtomicReader) topReader;
    final SortedDocValues sindex = FieldCache.DEFAULT.getTermsIndex(r, field);
    return new IntDocValues(this) {
      protected String toTerm(String readableValue) {
        return readableValue;
      }
      @Override
      public int intVal(int doc) {
        return sindex.getOrd(doc+off);
      }
      @Override
      public int ordVal(int doc) {
        return sindex.getOrd(doc+off);
      }
      @Override
      public int numOrd() {
        return sindex.getValueCount();
      }

      @Override
      public boolean exists(int doc) {
        return sindex.getOrd(doc+off) != 0;
      }

      @Override
      public ValueFiller getValueFiller() {
        return new ValueFiller() {
          private final MutableValueInt mval = new MutableValueInt();

          @Override
          public MutableValue getValue() {
            return mval;
          }

          @Override
          public void fillValue(int doc) {
            mval.value = sindex.getOrd(doc);
            mval.exists = mval.value!=0;
          }
        };
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    return o != null && o.getClass() == OrdFieldSource.class && this.field.equals(((OrdFieldSource)o).field);
  }

  private static final int hcode = OrdFieldSource.class.hashCode();
  @Override
  public int hashCode() {
    return hcode + field.hashCode();
  }

}
