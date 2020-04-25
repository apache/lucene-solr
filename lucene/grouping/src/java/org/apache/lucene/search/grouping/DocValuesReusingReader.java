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
package org.apache.lucene.search.grouping;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;

/**
 * Caches docValues for the given {@linkplain LeafReader}.
 * It only necessary when consumer retrieves same docValues many times per
 * segment. Returned docValues should be iterated forward only.
 * Caveat: {@link #getContext()} is completely misguiding for this class since 
 * it looses baseDoc, ord from underneath context.
 * @lucene.experimental   
 * */
class DocValuesReusingReader extends FilterLeafReader {

  @FunctionalInterface
  interface DVSupplier<T extends DocIdSetIterator>{
    T getDocValues(String field) throws IOException;
  } 
  
  private Map<String, ? super DocIdSetIterator> cache = new HashMap<>();

  DocValuesReusingReader(LeafReader in) {
    super(in);
  }

  @SuppressWarnings("unchecked")
  protected <T extends DocIdSetIterator> T computeIfAbsent(String field, DVSupplier<T> supplier) throws IOException {
    T dv;
    if ((dv = (T) cache.get(field)) == null) {
         dv = supplier.getDocValues(field);
         cache.put(field, dv);
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
    return computeIfAbsent(field, in::getSortedSetDocValues);
  }
  
}