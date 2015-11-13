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

package org.apache.solr.schema;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.docvalues.LongDocValues;
import org.apache.lucene.queries.function.valuesource.SortedSetFieldSource;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueLong;

/**
 * A numeric field that can contain 64-bit signed two's complement integer values.
 *
 * <ul>
 *  <li>Min Value Allowed: -9223372036854775808</li>
 *  <li>Max Value Allowed: 9223372036854775807</li>
 * </ul>
 * 
 * @see Long
 */
public class TrieLongField extends TrieField implements LongValueFieldType {
  {
    type=TrieTypes.LONG;
  }

  @Override
  public Object toNativeType(Object val) {
    if(val==null) return null;
    if (val instanceof Number) return ((Number) val).longValue();
    try {
      if (val instanceof String) return Long.parseLong((String) val);
    } catch (NumberFormatException e) {
      Double v = Double.parseDouble((String) val);
      return v.longValue();
    }
    return super.toNativeType(val);
  }

  @Override
  protected ValueSource getSingleValueSource(SortedSetSelector.Type choice, SchemaField f) {
    
    return new SortedSetFieldSource(f.getName(), choice) {
      @Override
      public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
        SortedSetFieldSource thisAsSortedSetFieldSource = this; // needed for nested anon class ref
        
        SortedSetDocValues sortedSet = DocValues.getSortedSet(readerContext.reader(), field);
        final SortedDocValues view = SortedSetSelector.wrap(sortedSet, selector);
        
        return new LongDocValues(thisAsSortedSetFieldSource) {
          @Override
          public long longVal(int doc) {
            BytesRef bytes = view.get(doc);
            if (0 == bytes.length) {
              // the only way this should be possible is for non existent value
              assert !exists(doc) : "zero bytes for doc, but exists is true";
              return 0L;
            }
            return NumericUtils.prefixCodedToLong(bytes);
          }

          @Override
          public boolean exists(int doc) {
            return -1 != view.getOrd(doc);
          }

          @Override
          public ValueFiller getValueFiller() {
            return new ValueFiller() {
              private final MutableValueLong mval = new MutableValueLong();
              
              @Override
              public MutableValue getValue() {
                return mval;
              }
              
              @Override
              public void fillValue(int doc) {
                // micro optimized (eliminate at least one redudnent ord check) 
                //mval.exists = exists(doc);
                //mval.value = mval.exists ? longVal(doc) : 0;
                //
                BytesRef bytes = view.get(doc);
                mval.exists = (0 == bytes.length);
                mval.value = mval.exists ? NumericUtils.prefixCodedToLong(bytes) : 0L;
              }
            };
          }
        };
      }
    };
  }
}
