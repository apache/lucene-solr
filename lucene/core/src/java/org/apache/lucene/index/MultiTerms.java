package org.apache.lucene.index;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.util.BytesRef;

import org.apache.lucene.util.automaton.CompiledAutomaton;

/**
 * Exposes flex API, merged from flex API of
 * sub-segments.
 *
 * @lucene.experimental
 */

public final class MultiTerms extends Terms {
  private final Terms[] subs;
  private final ReaderSlice[] subSlices;
  private final Comparator<BytesRef> termComp;
  private final boolean hasOffsets;
  private final boolean hasPositions;
  private final boolean hasPayloads;

  /** Sole constructor.
   *
   * @param subs The {@link Terms} instances of all sub-readers. 
   * @param subSlices A parallel array (matching {@code
   *        subs}) describing the sub-reader slices.
   */
  public MultiTerms(Terms[] subs, ReaderSlice[] subSlices) throws IOException {
    this.subs = subs;
    this.subSlices = subSlices;
    
    Comparator<BytesRef> _termComp = null;
    assert subs.length > 0 : "inefficient: don't use MultiTerms over one sub";
    boolean _hasOffsets = true;
    boolean _hasPositions = true;
    boolean _hasPayloads = false;
    for(int i=0;i<subs.length;i++) {
      if (_termComp == null) {
        _termComp = subs[i].getComparator();
      } else {
        // We cannot merge sub-readers that have
        // different TermComps
        final Comparator<BytesRef> subTermComp = subs[i].getComparator();
        if (subTermComp != null && !subTermComp.equals(_termComp)) {
          throw new IllegalStateException("sub-readers have different BytesRef.Comparators; cannot merge");
        }
      }
      _hasOffsets &= subs[i].hasOffsets();
      _hasPositions &= subs[i].hasPositions();
      _hasPayloads |= subs[i].hasPayloads();
    }

    termComp = _termComp;
    hasOffsets = _hasOffsets;
    hasPositions = _hasPositions;
    hasPayloads = hasPositions && _hasPayloads; // if all subs have pos, and at least one has payloads.
  }

  @Override
  public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
    final List<MultiTermsEnum.TermsEnumIndex> termsEnums = new ArrayList<MultiTermsEnum.TermsEnumIndex>();
    for(int i=0;i<subs.length;i++) {
      final TermsEnum termsEnum = subs[i].intersect(compiled, startTerm);
      if (termsEnum != null) {
        termsEnums.add(new MultiTermsEnum.TermsEnumIndex(termsEnum, i));
      }
    }

    if (termsEnums.size() > 0) {
      return new MultiTermsEnum(subSlices).reset(termsEnums.toArray(MultiTermsEnum.TermsEnumIndex.EMPTY_ARRAY));
    } else {
      return TermsEnum.EMPTY;
    }
  }

  @Override
  public TermsEnum iterator(TermsEnum reuse) throws IOException {

    final List<MultiTermsEnum.TermsEnumIndex> termsEnums = new ArrayList<MultiTermsEnum.TermsEnumIndex>();
    for(int i=0;i<subs.length;i++) {
      final TermsEnum termsEnum = subs[i].iterator(null);
      if (termsEnum != null) {
        termsEnums.add(new MultiTermsEnum.TermsEnumIndex(termsEnum, i));
      }
    }

    if (termsEnums.size() > 0) {
      return new MultiTermsEnum(subSlices).reset(termsEnums.toArray(MultiTermsEnum.TermsEnumIndex.EMPTY_ARRAY));
    } else {
      return TermsEnum.EMPTY;
    }
  }

  @Override
  public long size() {
    return -1;
  }

  @Override
  public long getSumTotalTermFreq() throws IOException {
    long sum = 0;
    for(Terms terms : subs) {
      final long v = terms.getSumTotalTermFreq();
      if (v == -1) {
        return -1;
      }
      sum += v;
    }
    return sum;
  }
  
  @Override
  public long getSumDocFreq() throws IOException {
    long sum = 0;
    for(Terms terms : subs) {
      final long v = terms.getSumDocFreq();
      if (v == -1) {
        return -1;
      }
      sum += v;
    }
    return sum;
  }
  
  @Override
  public int getDocCount() throws IOException {
    int sum = 0;
    for(Terms terms : subs) {
      final int v = terms.getDocCount();
      if (v == -1) {
        return -1;
      }
      sum += v;
    }
    return sum;
  }

  @Override
  public Comparator<BytesRef> getComparator() {
    return termComp;
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

