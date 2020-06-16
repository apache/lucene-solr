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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.solr.legacy.LegacyNumericUtils;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.FloatDocValues;
import org.apache.lucene.queries.function.valuesource.SortedSetFieldSource;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueFloat;

/**
 * A numeric field that can contain single-precision 32-bit IEEE 754 
 * floating point values.
 *
 * <ul>
 *  <li>Min Value Allowed: 1.401298464324817E-45</li>
 *  <li>Max Value Allowed: 3.4028234663852886E38</li>
 * </ul>
 *
 * <b>NOTE:</b> The behavior of this class when given values of 
 * {@link Float#NaN}, {@link Float#NEGATIVE_INFINITY}, or 
 * {@link Float#POSITIVE_INFINITY} is undefined.
 * 
 * @see Float
 * @see <a href="http://java.sun.com/docs/books/jls/third_edition/html/typesValues.html#4.2.3">Java Language Specification, s4.2.3</a>
 * @deprecated Trie fields are deprecated as of Solr 7.0
 */
@Deprecated
public class TrieFloatField extends TrieField implements FloatValueFieldType {
  {
    type = NumberType.FLOAT;
  }

  @Override
  public Object toNativeType(Object val) {
    if(val==null) return null;
    if (val instanceof Number) return ((Number) val).floatValue();
    if (val instanceof CharSequence) return Float.parseFloat(val.toString());
    return super.toNativeType(val);
  }

  @Override
  protected ValueSource getSingleValueSource(SortedSetSelector.Type choice, SchemaField f) {
    
    return new SortedSetFieldSource(f.getName(), choice) {
      @Override
      public FunctionValues getValues(@SuppressWarnings({"rawtypes"})Map context, LeafReaderContext readerContext) throws IOException {
        SortedSetFieldSource thisAsSortedSetFieldSource = this; // needed for nested anon class ref
        
        SortedSetDocValues sortedSet = DocValues.getSortedSet(readerContext.reader(), field);
        SortedDocValues view = SortedSetSelector.wrap(sortedSet, selector);
        
        return new FloatDocValues(thisAsSortedSetFieldSource) {
          private int lastDocID;

          private boolean setDoc(int docID) throws IOException {
            if (docID < lastDocID) {
              throw new IllegalArgumentException("docs out of order: lastDocID=" + lastDocID + " docID=" + docID);
            }
            if (docID > view.docID()) {
              return docID == view.advance(docID);
            } else {
              return docID == view.docID();
            }
          }
          
          @Override
          public float floatVal(int doc) throws IOException {
            if (setDoc(doc)) {
              BytesRef bytes = view.binaryValue();
              assert bytes.length > 0;
              return NumericUtils.sortableIntToFloat(LegacyNumericUtils.prefixCodedToInt(bytes));
            } else {
              return 0F;
            }
          }

          @Override
          public boolean exists(int doc) throws IOException {
            return setDoc(doc);
          }

          @Override
          public ValueFiller getValueFiller() {
            return new ValueFiller() {
              private final MutableValueFloat mval = new MutableValueFloat();
              
              @Override
              public MutableValue getValue() {
                return mval;
              }
              
              @Override
              public void fillValue(int doc) throws IOException {
                if (setDoc(doc)) {
                  mval.exists = true;
                  mval.value = NumericUtils.sortableIntToFloat(LegacyNumericUtils.prefixCodedToInt(view.binaryValue()));
                } else {
                  mval.exists = false;
                  mval.value = 0F;
                }
              }
            };
          }
        };
      }
    };
  }

}
