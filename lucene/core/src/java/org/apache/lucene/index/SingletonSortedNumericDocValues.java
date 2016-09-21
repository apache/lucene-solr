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


/** 
 * Exposes multi-valued view over a single-valued instance.
 * <p>
 * This can be used if you want to have one multi-valued implementation
 * that works for single or multi-valued types.
 */
final class SingletonSortedNumericDocValues extends SortedNumericDocValues {
  private final NumericDocValues in;
  private long value;
  
  public SingletonSortedNumericDocValues(NumericDocValues in) {
    if (in.docID() != -1) {
      throw new IllegalStateException("iterator has already been used: docID=" + in.docID());
    }
    this.in = in;
  }

  /** Return the wrapped {@link NumericDocValues} */
  public NumericDocValues getNumericDocValues() {
    if (in.docID() != -1) {
      throw new IllegalStateException("iterator has already been used: docID=" + in.docID());
    }
    return in;
  }

  @Override
  public int docID() {
    return in.docID();
  }

  @Override
  public int nextDoc() throws IOException {
    int docID = in.nextDoc();
    if (docID != NO_MORE_DOCS) {
      value = in.longValue();
    }
    return docID;
  }
  
  @Override
  public int advance(int target) throws IOException {
    int docID = in.advance(target);
    if (docID != NO_MORE_DOCS) {
      value = in.longValue();
    }
    return docID;
  }
      
  @Override
  public long cost() {
    return in.cost();
  }
  
  @Override
  public long nextValue() {
    return value;
  }

  @Override
  public int docValueCount() {
    return 1;
  }
}
