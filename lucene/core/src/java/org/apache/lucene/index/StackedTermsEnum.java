package org.apache.lucene.index;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
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

/**
 * TermsEnum for stacked segments (with updates). At the term level the terms
 * are merged without taking into consideration fields replacements, so terms
 * with no occurrences might return. Given a certain term, all the enumerations
 * take into account fields replacements.
 */
class StackedTermsEnum extends TermsEnum {
  
  private final Terms[] subTerms;
  private final FieldGenerationReplacements replacements;
  private Comparator<BytesRef> comparator;
  private TreeSet<InnerTermsEnum> activeEnums;
  
  protected StackedTermsEnum(Terms[] subTerms,
      FieldGenerationReplacements replacements, Comparator<BytesRef> comparator)
      throws IOException {
    this.subTerms = subTerms;
    this.replacements = replacements;
    this.comparator = comparator;
  }
  
  @Override
  public Comparator<BytesRef> getComparator() {
    return comparator;
  }
  
  @Override
  public BytesRef next() throws IOException {
    if (activeEnums == null) {
      init();
      return headTerm();
    }
    
    // get the current term (queue head)
    BytesRef headTerm = headTerm();
    final BytesRef currentHead = BytesRef.deepCopyOf(headTerm);
    
    // advance all enums with same term
    while (currentHead.equals(headTerm)) {
      if (activeEnums.isEmpty()) {
        return null;
      } else {
        final InnerTermsEnum polled = activeEnums.pollFirst();
        if (polled.advance()) {
          activeEnums.add(polled);
        }
        if (activeEnums.isEmpty()) {
          // done, return null
          headTerm = null;
        } else {
          // still active, move to next enum
          headTerm = headTerm();
        }
      }
    }
    
    return headTerm;
  }
  
  private void init() throws IOException {
    activeEnums = new TreeSet<InnerTermsEnum>(new InnerTermsEnumFullComparator());
    for (int i = 0; i < subTerms.length; i++) {
      if (subTerms[i] != null) {
        final TermsEnum termsEnum = subTerms[i].iterator(null);
        final BytesRef term = termsEnum.next();
        if (term != null) {
          activeEnums.add(new InnerTermsEnum(i, termsEnum, term));
        }
      }
    }
  }
  
  @Override
  public BytesRef term() throws IOException {
    return headTerm();
  }
  
  private BytesRef headTerm() {
    final InnerTermsEnum head = activeEnums.first();
    if (head == null) {
      return null;
    }
    return head.getTerm();
  }
  
  @Override
  public SeekStatus seekCeil(BytesRef text, boolean useCache)
      throws IOException {
    // reset active enums
    if (activeEnums == null) {
      activeEnums = new TreeSet<InnerTermsEnum>(new InnerTermsEnumFullComparator());
    } else {
      activeEnums.clear();
    }
    
    // do seekCeil on all non-null subTerms
    SeekStatus status = SeekStatus.END;
    for (int i = 0; i < subTerms.length; i++) {
      if (subTerms[i] != null) {
        final TermsEnum termsEnum = subTerms[i].iterator(null);
        final SeekStatus tempStatus = termsEnum.seekCeil(text, useCache);
        if (tempStatus != SeekStatus.END) {
          // put in new queue
          activeEnums.add(new InnerTermsEnum(i, termsEnum, termsEnum.term()));
          
          // update status if needed
          if (tempStatus == SeekStatus.FOUND) {
            status = SeekStatus.FOUND;
          } else if (status == SeekStatus.END) {
            status = SeekStatus.NOT_FOUND;
          }
        }
      }
    }
    return status;
  }
  
  @Override
  public long ord() throws IOException {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public void seekExact(long ord) throws IOException {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public int docFreq() throws IOException {
    final DocsEnum docs = docs(null, null, 0);
    int docFreq = 0;
    while (docs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      docFreq++;
    }
    return docFreq;
  }
  
  @Override
  public long totalTermFreq() throws IOException {
    final DocsEnum docsEnum = docs(null, null);
    int totalTermFreq = 0;
    if (docsEnum != null) {
      while (docsEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        totalTermFreq += docsEnum.freq();
      }
    }
    return totalTermFreq;
  }
  
  @Override
  public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags)
      throws IOException {
    // build map of active enums with indexes
    Map<DocsEnum,Integer> activeMap = new HashMap<DocsEnum,Integer>();

    // iterate over active enums, fetch DocsEnum of all those pointing to the
    // next term
    InnerTermsEnum first = activeEnums.first();
    Iterator<InnerTermsEnum> iterator = activeEnums.iterator();
    while (iterator != null && iterator.hasNext()) {
      InnerTermsEnum inner = iterator.next();
      if (comparator.compare(first.term, inner.term) == 0) {
        final DocsEnum docs = inner.termsEnum.docs(liveDocs, reuse, flags);
        if (docs != null) {
          activeMap.put(docs, inner.getIndex());
        }
      } else {
        iterator = null;
      }
    }

    if (activeMap.isEmpty()) {
      return null;
    }
    
    if (replacements == null && activeMap.size() == 1) {
      return activeMap.keySet().iterator().next();
    }

    return new StackedDocsEnum(activeMap, replacements);
  }
  
  @Override
  public DocsAndPositionsEnum docsAndPositions(Bits liveDocs,
      DocsAndPositionsEnum reuse, int flags) throws IOException {
    // build map of active enums with indexes
    Map<DocsEnum,Integer> activeMap = new HashMap<DocsEnum,Integer>();

    // iterate over active enums, fetch DocsAndPositionsEnum of all those
    // pointing to the next term
    InnerTermsEnum first = activeEnums.first();
    Iterator<InnerTermsEnum> iterator = activeEnums.iterator();
    while (iterator != null && iterator.hasNext()) {
      InnerTermsEnum inner = iterator.next();
      if (comparator.compare(first.term, inner.term) == 0) {
        final DocsAndPositionsEnum docsAndPositions = inner.termsEnum
            .docsAndPositions(liveDocs, reuse, flags);
        if (docsAndPositions != null) {
          activeMap.put(docsAndPositions, inner.getIndex());
        }
      } else {
        iterator = null;
      }
    }
    
    if (activeMap.isEmpty()) {
      return null;
    }
    
    if (replacements == null && activeMap.size() == 1) {
      return (DocsAndPositionsEnum) activeMap.keySet().iterator().next();
    }

    return new StackedDocsEnum(activeMap, replacements);
  }
  
  private class InnerTermsEnum implements Comparable<InnerTermsEnum> {
    
    private int index;
    private TermsEnum termsEnum;
    private BytesRef term;
    
    public InnerTermsEnum(int index, TermsEnum termsEnum, BytesRef term) {
      this.index = index;
      this.termsEnum = termsEnum;
      this.term = term;
    }
    
    public int getIndex() {
      return index;
    }
    
    public BytesRef getTerm() {
      return term;
    }
    
    public boolean advance() throws IOException {
      term = termsEnum.next();
      return term != null;
    }
    
    @Override
    public int compareTo(InnerTermsEnum o) {
      int diff = comparator.compare(this.term, o.term);
      if (diff != 0) {
        return diff;
      }
      return this.index - o.index;

    }
    
  }

  /**
   * A comparator which 
   */
  private class InnerTermsEnumFullComparator implements Comparator<InnerTermsEnum> {

    @Override
    public int compare(InnerTermsEnum arg0, InnerTermsEnum arg1) {
      int diff = comparator.compare(arg0.term, arg1.term);
      if (diff != 0) {
        return diff;
      }
      return arg0.index - arg1.index;
    }
    
  }
}
