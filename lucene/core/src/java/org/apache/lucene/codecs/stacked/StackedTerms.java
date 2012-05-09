package org.apache.lucene.codecs.stacked;

import java.io.IOException;
import java.util.Comparator;

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

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

public class StackedTerms extends Terms {
  Terms main, stacked;
  StackedMap map;
  Comparator<BytesRef> cmp;
  String field;
  
  StackedTerms(String field, Terms mainTerms, Terms stackedTerms, StackedMap map) throws IOException {
    this.main = mainTerms;
    this.stacked = stackedTerms;
    this.field = field;
    assert this.stacked != null;
    this.map = map;
    if (main != null) {
      cmp = main.getComparator();
    } else {
      cmp = stacked.getComparator();
    }
  }
  
  @Override
  public TermsEnum iterator(TermsEnum reuse) throws IOException {
    return new StackedTermsEnum(reuse);
  }
  
  // Leapfrog enumerator. Iterates (or seeks) to the smallest next term from
  // either the main or the stacked enum, and continues to enumerate from
  // that enum until it reaches the other enum's term.
  // When terms are equal (i.e. there are updated for a term) then a combined
  // Doc*Enum is returned.
  class StackedTermsEnum extends TermsEnum {
    TermsEnum mte = null, ste = null;
    BytesRef lastMTerm = null;
    BytesRef lastSTerm = null;
    boolean inited = false;
    boolean needsSNext = false, needsMNext = false;
    int m_lessThan_s = 1;
    
    StackedTermsEnum(TermsEnum reuse) throws IOException {
      if (main != null) {
        if (reuse instanceof StackedTermsEnum) {
          mte = main.iterator(((StackedTermsEnum)reuse).mte);
        } else {
          mte = main.iterator(reuse);
        }
      }
      ste = stacked.iterator(null);
    }

    @Override
    public BytesRef next() throws IOException {
      if (!inited) {
        lastSTerm = ste.next();
        if (mte != null) {
          lastMTerm = mte.next();
        }
        inited = true;
      } else {
        if (m_lessThan_s < 0) {
          if (mte != null) lastMTerm = mte.next();
        } else if (m_lessThan_s > 0) {
          lastSTerm = ste.next();
        } else {
          lastSTerm = ste.next();
          if (mte != null) lastMTerm = mte.next();
        }
      }
      if (lastMTerm != null && lastSTerm != null) {
        m_lessThan_s = cmp.compare(lastMTerm, lastSTerm);
        if (m_lessThan_s == 0) { // postings for the same term
          return lastMTerm; // return whichever
        } else if (m_lessThan_s < 0) { // postings from m go now
          return lastMTerm;          
        } else { // postings from s go now
          return lastSTerm;
        }
      } else {
        if (lastMTerm != null) {
          return lastMTerm;
        }
        if (lastSTerm != null) {
          return lastSTerm;
        }
        return null;
      }
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      return cmp;
    }

    @Override
    public SeekStatus seekCeil(BytesRef text, boolean useCache)
        throws IOException {
      SeekStatus sStatus = ste.seekCeil(text, useCache);
      lastSTerm = ste.term();
      SeekStatus mStatus = SeekStatus.END;
      if (mte != null) {
         mStatus = mte.seekCeil(text, useCache);
         lastMTerm = mte.term();
      }
      if (sStatus == SeekStatus.FOUND || mStatus == SeekStatus.FOUND) {
        return SeekStatus.FOUND;
      } else if (mStatus == SeekStatus.NOT_FOUND || sStatus == SeekStatus.NOT_FOUND) {
        return SeekStatus.NOT_FOUND;
      } else {
        return SeekStatus.END;
      }
    }

    // XXX
    @Override
    public void seekExact(long ord) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public BytesRef term() throws IOException {
      if (m_lessThan_s == 0) {
        return lastMTerm;
      } else if (m_lessThan_s > 0) {
        return lastSTerm;
      } else {
        return lastMTerm;
      }
    }

    // XXX
    @Override
    public long ord() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int docFreq() throws IOException {
      if (m_lessThan_s == 0) {
        return mte.docFreq(); // XXX doesn't consider stacked docFreq
      } else if (m_lessThan_s > 0) {
        return ste.docFreq();
      } else {
        return mte.docFreq();
      }
    }

    @Override
    public long totalTermFreq() throws IOException {
      if (m_lessThan_s == 0) {
        return mte.totalTermFreq(); // XXX doesn't consider stacked docFreq
      } else if (m_lessThan_s > 0) {
        return ste.totalTermFreq();
      } else {
        return mte.totalTermFreq();
      }
    }

    @Override
    public DocsEnum docs(Bits liveDocs, DocsEnum reuse, boolean needsFreqs)
        throws IOException {
      if (m_lessThan_s == 0) { // merge old and new postings
        return new StackedDocsEnum(field, map, lastSTerm, mte.docs(liveDocs, null, needsFreqs), ste, needsFreqs, liveDocs);
      } else if (m_lessThan_s > 0) { // return postings for stacked, remapping
        return new StackedDocsEnum(field, map, lastSTerm, null, ste, needsFreqs, liveDocs);
      } else { // return original postings
        return mte.docs(liveDocs, reuse, needsFreqs);
      }
    }

    @Override
    public DocsAndPositionsEnum docsAndPositions(Bits liveDocs,
        DocsAndPositionsEnum reuse, boolean needsOffsets) throws IOException {
      if (m_lessThan_s == 0) { // merge old and new postings
        return new StackedDocsAndPositionsEnum(field, map, lastSTerm, mte.docsAndPositions(liveDocs, null, needsOffsets), ste, needsOffsets, liveDocs);
      } else if (m_lessThan_s > 0) { // return postings for stacked, remapping
        return new StackedDocsAndPositionsEnum(field, map, lastSTerm, null, ste, needsOffsets, liveDocs);
      } else { // return original postings
        return mte.docsAndPositions(liveDocs, reuse, needsOffsets);
      }
    }
    
  }
  
  @Override
  public Comparator<BytesRef> getComparator() throws IOException {
    return cmp;
  }
  
  // XXX stats?
  @Override
  public long size() throws IOException {
    if (main != null) {
      return main.size();
    } else {
      return stacked.size();
    }
  }
  
  // XXX stats?
  @Override
  public long getSumTotalTermFreq() throws IOException {
    if (main != null) {
      return main.getSumTotalTermFreq();
    } else {
      return stacked.getSumTotalTermFreq();
    }
  }
  
  // XXX stats?
  @Override
  public long getSumDocFreq() throws IOException {
    if (main != null) {
      return main.getSumDocFreq();
    } else {
      return stacked.getSumDocFreq();
    }
  }
  
  // XXX stats?
  @Override
  public int getDocCount() throws IOException {
    if (main != null) {
      return main.getDocCount();
    } else {
      return stacked.getDocCount();
    }
  }
  
}
