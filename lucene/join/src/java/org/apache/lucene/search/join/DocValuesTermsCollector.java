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
package org.apache.lucene.search.join;

import java.io.IOException;

import org.apache.lucene.document.FieldType.NumericType;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;

abstract class DocValuesTermsCollector<DV> extends SimpleCollector {
  
  static interface Function<R> {
      R apply(LeafReader t) throws IOException  ;
  }
  
  static interface LongConsumer {
    void accept(long value);
  }
  
  protected DV docValues;
  private final Function<DV> docValuesCall;
  
  public DocValuesTermsCollector(Function<DV> docValuesCall) {
    this.docValuesCall = docValuesCall;
  }

  @Override
  protected final void doSetNextReader(LeafReaderContext context) throws IOException {
    docValues = docValuesCall.apply(context.reader());
  }
  
  static Function<BinaryDocValues> binaryDocValues(final String field) {
      return new Function<BinaryDocValues>() 
      {
        @Override
        public BinaryDocValues apply(LeafReader ctx) throws IOException {
          return DocValues.getBinary(ctx, field);
        }
      };
  }
  static Function<SortedSetDocValues> sortedSetDocValues(final String field) {
    return new Function<SortedSetDocValues>() 
    {
      @Override
      public SortedSetDocValues apply(LeafReader ctx) throws IOException {
        return DocValues.getSortedSet(ctx, field);
      }
    };
  }
  
  static Function<BinaryDocValues> numericAsBinaryDocValues(final String field, final NumericType numTyp) {
    return new Function<BinaryDocValues>() {
      @Override
      public BinaryDocValues apply(LeafReader ctx) throws IOException {
        final NumericDocValues numeric = DocValues.getNumeric(ctx, field);
        final BytesRefBuilder bytes = new BytesRefBuilder();
        
        final LongConsumer coder = coder(bytes, numTyp, field);
        
        return new BinaryDocValues() {
          @Override
          public BytesRef get(int docID) {
            final long lVal = numeric.get(docID);
            coder.accept(lVal);
            return bytes.get();
          }
        };
      }
    };
  }
  
  static LongConsumer coder(final BytesRefBuilder bytes, NumericType type, String fieldName){
    switch(type){
      case INT: 
        return new LongConsumer() {
          @Override
          public void accept(long value) {
            NumericUtils.intToPrefixCoded((int)value, 0, bytes);
          }
        };
      case LONG: 
        return new LongConsumer() {
          @Override
          public void accept(long value) {
            NumericUtils.longToPrefixCoded((int)value, 0, bytes);
          }
        };
      default:
        throw new IllegalArgumentException("Unsupported "+type+
            ". Only "+NumericType.INT+" and "+NumericType.LONG+" are supported."
            + "Field "+fieldName );
    }
  }
  
  /** this adapter is quite weird. ords are per doc index, don't use ords across different docs*/
  static Function<SortedSetDocValues> sortedNumericAsSortedSetDocValues(final String field, final NumericType numTyp) {
    return new Function<SortedSetDocValues>() {
      @Override
      public SortedSetDocValues apply(LeafReader ctx) throws IOException {
        final SortedNumericDocValues numerics = DocValues.getSortedNumeric(ctx, field);
        final BytesRefBuilder bytes = new BytesRefBuilder();
        
        final LongConsumer coder = coder(bytes, numTyp, field);
        
        return new SortedSetDocValues() {
          
          private int index = Integer.MIN_VALUE;
          
          @Override
          public long nextOrd() {
            return index < numerics.count() - 1 ? ++index : NO_MORE_ORDS;
          }
          
          @Override
          public void setDocument(int docID) {
            numerics.setDocument(docID);
            index = -1;
          }
          
          @Override
          public BytesRef lookupOrd(long ord) {
            assert ord >= 0 && ord < numerics.count();
            final long value = numerics.valueAt((int) ord);
            coder.accept(value);
            return bytes.get();
          }
          
          @Override
          public long getValueCount() {
            throw new UnsupportedOperationException("it's just number encoding wrapper");
          }
          
          @Override
          public long lookupTerm(BytesRef key) {
            throw new UnsupportedOperationException("it's just number encoding wrapper");
          }
        };
      }
    };
  }
}
