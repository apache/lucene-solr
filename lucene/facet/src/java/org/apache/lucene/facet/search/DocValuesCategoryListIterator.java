package org.apache.lucene.facet.search;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValues.Source;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.encoding.IntDecoder;

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

/** A {@link CategoryListIterator} which reads the ordinals from a {@link DocValues}. */
public class DocValuesCategoryListIterator implements CategoryListIterator {
  
  private final IntDecoder decoder;
  private final String field;
  private final int hashCode;
  private final boolean useDirectSource;
  private final BytesRef bytes = new BytesRef(32);
  
  private DocValues.Source current;
  
  /**
   * Constructs a new {@link DocValuesCategoryListIterator} which uses an
   * in-memory {@link Source}.
   */
  public DocValuesCategoryListIterator(String field, IntDecoder decoder) {
    this(field, decoder, false);
  }
  
  /**
   * Constructs a new {@link DocValuesCategoryListIterator} which uses either a
   * {@link DocValues#getDirectSource() direct source} or
   * {@link DocValues#getSource() in-memory} one.
   */
  public DocValuesCategoryListIterator(String field, IntDecoder decoder, boolean useDirectSource) {
    this.field = field;
    this.decoder = decoder;
    this.hashCode = field.hashCode();
    this.useDirectSource = useDirectSource;
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
    DocValues dv = context.reader().docValues(field);
    if (dv == null) {
      current = null;
      return false;
    }
    
    current = useDirectSource ? dv.getDirectSource() : dv.getSource();
    return true;
  }
  
  @Override
  public void getOrdinals(int docID, IntsRef ints) throws IOException {
    current.getBytes(docID, bytes);
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
