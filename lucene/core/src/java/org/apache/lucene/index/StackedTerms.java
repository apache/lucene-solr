package org.apache.lucene.index;

import java.io.IOException;
import java.util.Comparator;

import org.apache.lucene.util.BytesRef;

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

public class StackedTerms extends Terms {
  
  private final FieldGenerationReplacements replacements;
  private final Terms[] subTerms;
  private Comparator<BytesRef> comparator;
  private boolean hasOffsets;
  private boolean hasPositions;
  private boolean hasPayloads;
  
  public StackedTerms(int maxTerms,
      FieldGenerationReplacements fieldGenerationReplacements) {
    this.replacements = fieldGenerationReplacements;
    subTerms = new Terms[maxTerms];
    
    hasOffsets = false;
    hasPositions = false;
    hasPayloads = false;
  }
  
  public void addTerms(Terms terms, int generation) throws IOException {
    if (terms != null) {
      subTerms[generation] = terms;
      hasOffsets |= terms.hasOffsets();
      hasPositions |= terms.hasPositions();
      hasPayloads |= terms.hasPayloads();
      if (comparator == null) {
        comparator = terms.getComparator();
      } else if (!comparator.equals(terms.getComparator())) {
        throw new IllegalStateException(
            "sub-readers have different BytesRef.Comparators; cannot merge");
      }
    }
  }
  
  @Override
  public TermsEnum iterator(TermsEnum reuse) throws IOException {
    return new StackedTermsEnum(subTerms, replacements, comparator);
  }
  
  @Override
  public Comparator<BytesRef> getComparator() throws IOException {
    if (comparator == null) {
      for (int i = 0; i < subTerms.length; i++) {
        if (subTerms[i] != null) {
          comparator = subTerms[i].getComparator();
          if (comparator != null) {
            return comparator;
          }
        }
      }
    }
    return comparator;
  }
  
  @Override
  public long size() throws IOException {
    final TermsEnum iterator = iterator(null);
    int size = 0;
    while (iterator.next() != null) {
      size++;
    }
    return size;
  }
  
  @Override
  public long getSumTotalTermFreq() throws IOException {
    long sum = 0;
    final TermsEnum iterator = iterator(null);
    while(iterator.next() != null) {
      sum += iterator.totalTermFreq(); 
    }
    if (sum == 0) {
      return -1;
    }
    return sum;
  }
  
  @Override
  public long getSumDocFreq() throws IOException {
    long sum = 0;
    final TermsEnum iterator = iterator(null);
    while(iterator.next() != null) {
      sum += iterator.docFreq(); 
    }
    if (sum == 0) {
      return -1;
    }
    return sum;
  }
  
  @Override
  public int getDocCount() throws IOException {
    // TODO: SY: can we actually compute this
    return -1;
  }
  
  @Override
  public boolean hasOffsets() {
    return hasOffsets;
  }
  
  @Override
  public boolean hasPositions() {
    return hasPositions;
  }
  
  @Override
  public boolean hasPayloads() {
    return hasPayloads;
  }
  
}
