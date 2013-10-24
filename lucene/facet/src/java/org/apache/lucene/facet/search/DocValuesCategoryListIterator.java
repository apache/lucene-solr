package org.apache.lucene.facet.search;

import java.io.IOException;

import org.apache.lucene.facet.encoding.IntDecoder;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;

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

/** A {@link CategoryListIterator} which reads the ordinals from a {@link BinaryDocValues}. */
public class DocValuesCategoryListIterator implements CategoryListIterator {
  
  private final IntDecoder decoder;
  private final String field;
  private final int hashCode;
  private final BytesRef bytes = new BytesRef(32);
  
  private BinaryDocValues current;
  
  /**
   * Constructs a new {@link DocValuesCategoryListIterator}.
   */
  public DocValuesCategoryListIterator(String field, IntDecoder decoder) {
    this.field = field;
    this.decoder = decoder;
    this.hashCode = field.hashCode();
  }
  
  @Override
  public int hashCode() {
    return hashCode;
  }
  
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof DocValuesCategoryListIterator)) {
      return false;
    }
    DocValuesCategoryListIterator other = (DocValuesCategoryListIterator) o;
    if (hashCode != other.hashCode) {
      return false;
    }
    
    // Hash codes are the same, check equals() to avoid cases of hash-collisions.
    return field.equals(other.field);
  }
  
  @Override
  public boolean setNextReader(AtomicReaderContext context) throws IOException {
    current = context.reader().getBinaryDocValues(field);
    return current != null;
  }
  
  @Override
  public void getOrdinals(int docID, IntsRef ints) throws IOException {
    assert current != null : "don't call this if setNextReader returned false";
    current.get(docID, bytes);
    ints.length = 0;
    if (bytes.length > 0) {
      decoder.decode(bytes, ints);
    }
  }
  
  @Override
  public String toString() {
    return field;
  }
  
}
