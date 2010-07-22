package org.apache.lucene.index;

/**
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

import java.util.*;

import org.apache.lucene.util.BytesRef;


class SegmentTermVector implements TermFreqVector {
  private String field;
  private BytesRef terms[];
  private int termFreqs[];
  
  SegmentTermVector(String field, BytesRef terms[], int termFreqs[]) {
    this.field = field;
    this.terms = terms;
    this.termFreqs = termFreqs;
  }

  /**
   * 
   * @return The number of the field this vector is associated with
   */
  public String getField() {
    return field;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append('{');
    sb.append(field).append(": ");
    if(terms != null){
      for (int i=0; i<terms.length; i++) {
        if (i>0) sb.append(", ");
        sb.append(terms[i]).append('/').append(termFreqs[i]);
      }
    }
    sb.append('}');
    
    return sb.toString();
  }

  public int size() {
    return terms == null ? 0 : terms.length;
  }

  public BytesRef [] getTerms() {
    return terms;
  }

  public int[] getTermFrequencies() {
    return termFreqs;
  }

  public int indexOf(BytesRef termBytes) {
    if(terms == null)
      return -1;
    int res = Arrays.binarySearch(terms, termBytes);
    return res >= 0 ? res : -1;
  }

  public int[] indexesOf(BytesRef [] termNumbers, int start, int len) {
    // TODO: there must be a more efficient way of doing this.
    //       At least, we could advance the lower bound of the terms array
    //       as we find valid indexes. Also, it might be possible to leverage
    //       this even more by starting in the middle of the termNumbers array
    //       and thus dividing the terms array maybe in half with each found index.
    int res[] = new int[len];

    for (int i=0; i < len; i++) {
      res[i] = indexOf(termNumbers[start+ i]);
    }
    return res;
  }
}
