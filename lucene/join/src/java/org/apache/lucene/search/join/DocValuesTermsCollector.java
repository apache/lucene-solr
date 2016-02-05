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
import java.util.function.LongConsumer;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FieldType.LegacyNumericType;
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
import org.apache.lucene.util.LegacyNumericUtils;

abstract class DocValuesTermsCollector<DV> extends SimpleCollector {
  
  @FunctionalInterface
  static interface Function<R> {
      R apply(LeafReader t) throws IOException  ;
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
  
  static Function<BinaryDocValues> binaryDocValues(String field) {
      return (ctx) -> DocValues.getBinary(ctx, field);
  }
  static Function<SortedSetDocValues> sortedSetDocValues(String field) {
    return (ctx) -> DocValues.getSortedSet(ctx, field);
  }
  
  static Function<BinaryDocValues> numericAsBinaryDocValues(String field, LegacyNumericType numTyp) {
    return (ctx) -> {
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
    };
  }
  
  static LongConsumer coder(BytesRefBuilder bytes, LegacyNumericType type, String fieldName){
    switch(type){
      case INT: 
        return (l) -> LegacyNumericUtils.intToPrefixCoded((int) l, 0, bytes);
      case LONG: 
        return (l) -> LegacyNumericUtils.longToPrefixCoded(l, 0, bytes);
      default:
        throw new IllegalArgumentException("Unsupported "+type+
            ". Only "+ LegacyNumericType.INT+" and "+ FieldType.LegacyNumericType.LONG+" are supported."
            + "Field "+fieldName );
    }
  }
  
  /** this adapter is quite weird. ords are per doc index, don't use ords across different docs*/
  static Function<SortedSetDocValues> sortedNumericAsSortedSetDocValues(String field, FieldType.LegacyNumericType numTyp) {
    return (ctx) -> {
      final SortedNumericDocValues numerics = DocValues.getSortedNumeric(ctx, field);
      final BytesRefBuilder bytes = new BytesRefBuilder();
      
      final LongConsumer coder = coder(bytes, numTyp, field);
      
      return new SortedSetDocValues() {

        private int index = Integer.MIN_VALUE;

        @Override
        public long nextOrd() {
          return index < numerics.count()-1 ? ++index : NO_MORE_ORDS;
        }

        @Override
        public void setDocument(int docID) {
          numerics.setDocument(docID);
          index=-1;
        }

        @Override
        public BytesRef lookupOrd(long ord) {
          assert ord>=0 && ord<numerics.count();
          final long value = numerics.valueAt((int)ord);
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
    };
  }
}
