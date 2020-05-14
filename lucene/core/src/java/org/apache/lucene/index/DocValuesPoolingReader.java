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
package org.apache.lucene.index;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

/**
 * Caches docValues for the given {@linkplain LeafReader}.
 * It only necessary when consumer retrieves same docValues many times per
 * segment. Returned docValues should be iterated forward only.
 * Caveat: {@link #getContext()} is completely misguiding for this class since 
 * it looses baseDoc, ord from underneath context.
 * @lucene.experimental   
 * */
public class DocValuesPoolingReader extends FilterLeafReader {

  @FunctionalInterface
  protected interface DVSupplier<T extends DocIdSetIterator>{
    T getDocValues(String field) throws IOException;
  } 
  
  private final Map<String, ? super DocIdSetIterator> cache = new HashMap<>();
  private final LeafReaderContext context;

  /**
   * Wraps the reader of the given context. Obtaining docBase and so ones from the given context
   * */
  public DocValuesPoolingReader(LeafReaderContext leafCtx) {
    super(leafCtx.reader());
    context = new LeafReaderContext(leafCtx.parent, this, leafCtx.ordInParent, leafCtx.docBaseInParent, leafCtx.ord, leafCtx.docBase);
  }

  public LeafReaderContext getPoolingContext() {
    return context;
  }
  
  @SuppressWarnings("unchecked")
  protected <T extends DocIdSetIterator> T computeIfAbsent(String field, DVSupplier<T> supplier) throws IOException {
    T dv;
    if ((dv = (T) cache.get(field)) == null) {
         dv = supplier.getDocValues(field);
         if (dv!=null) { // if (!dv.getClass().toString().contains("FieldCache")) { it make no sense to cache FieldCache, but it works.
           cache.put(field, dv);
         }
    }
    return dv;
  }

  @Override
  public CacheHelper getReaderCacheHelper() {
    return null;
  }

  @Override
  public CacheHelper getCoreCacheHelper() {
    return null;
  }

  @Override
  public BinaryDocValues getBinaryDocValues(String field) throws IOException {
    return computeIfAbsent(field, in::getBinaryDocValues);
  }
  
  @Override
  public NumericDocValues getNumericDocValues(String field) throws IOException {
    return computeIfAbsent(field, in::getNumericDocValues);
  }
  
  @Override
  public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
    return computeIfAbsent(field, in::getSortedNumericDocValues);
  }
  
  public SortedDocValues getSortedDocValues(String field) throws IOException {
    return computeIfAbsent(field, in::getSortedDocValues);
  }
  
  @Override
  public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
    return computeIfAbsent(field, field1 -> {
      final SortedSetDocValues sortedSet = in.getSortedSetDocValues(field1);
      final SortedDocValues singleton = DocValues.unwrapSingleton(sortedSet);
      if (singleton!=null) {
        return new SortedSetDocValues() {
          private long ord;
          
          @Override
          public int docID() {
            return singleton.docID();
          }

          @Override
          public long nextOrd() {
            long v = ord;
            ord = NO_MORE_ORDS;
            return v;
          }

          @Override
          public int nextDoc() throws IOException {
            int docID = singleton.nextDoc();
            if (docID != NO_MORE_DOCS) {
              ord = singleton.ordValue();
            }
            return docID;
          }

          @Override
          public int advance(int target) throws IOException {
            int docID = singleton.advance(target);
            if (docID != NO_MORE_DOCS) {
              ord = singleton.ordValue();
            }
            return docID;
          }

          @Override
          public boolean advanceExact(int target) throws IOException {
            if (singleton.advanceExact(target)) {
              ord = singleton.ordValue();
              return true;
            }
            return false;
          }

          @Override
          public BytesRef lookupOrd(long ord) throws IOException {
            // cast is ok: single-valued cannot exceed Integer.MAX_VALUE
            return singleton.lookupOrd((int) ord);
          }

          @Override
          public long getValueCount() {
            return singleton.getValueCount();
          }

          @Override
          public long lookupTerm(BytesRef key) throws IOException {
            return singleton.lookupTerm(key);
          }

          @Override
          public TermsEnum termsEnum() throws IOException {
            return singleton.termsEnum();
          }

          @Override
          public long cost() {
            return singleton.cost();
          }
        };
      } else {
        return sortedSet;
      }
    });
  }
  
}