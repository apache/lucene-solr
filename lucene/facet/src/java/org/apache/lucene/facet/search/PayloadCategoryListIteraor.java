package org.apache.lucene.facet.search;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Term;
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

/**
 * A {@link CategoryListIterator} which reads the category ordinals from a
 * payload.
 * 
 * @lucene.experimental
 */
public class PayloadCategoryListIteraor implements CategoryListIterator {

  private final IntDecoder decoder;
  private final Term term;
  private final PayloadIterator pi;
  private final int hashCode;
  
  public PayloadCategoryListIteraor(Term term, IntDecoder decoder) throws IOException {
    pi = new PayloadIterator(term);
    this.decoder = decoder;
    hashCode = term.hashCode();
    this.term = term;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof PayloadCategoryListIteraor)) {
      return false;
    }
    PayloadCategoryListIteraor that = (PayloadCategoryListIteraor) other;
    if (hashCode != that.hashCode) {
      return false;
    }
    
    // Hash codes are the same, check equals() to avoid cases of hash-collisions.
    return term.equals(that.term);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public boolean setNextReader(AtomicReaderContext context) throws IOException {
    return pi.setNextReader(context);
  }
  
  @Override
  public void getOrdinals(int docID, IntsRef ints) throws IOException {
    ints.length = 0;
    BytesRef payload = pi.getPayload(docID);
    if (payload != null) {
      decoder.decode(payload, ints);
    }
  }

}
