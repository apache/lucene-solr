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

import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BitsSlice;
import org.apache.lucene.util.MultiBits;
import org.apache.lucene.util.ReaderUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Exposes flex API, merged from flex API of sub-segments.
 * This does a merge sort, by term text, of the sub-readers.
 *
 * @lucene.experimental
 */
public final class MultiTermsEnum extends TermsEnum {
    
  private final TermMergeQueue queue;
  private final TermsEnumWithSlice[] subs;        // all of our subs (one per sub-reader)
  private final TermsEnumWithSlice[] currentSubs; // current subs that have at least one term for this field
  private final TermsEnumWithSlice[] top;
  private final MultiDocsEnum.EnumWithSlice[] subDocs;
  private final MultiDocsAndPositionsEnum.EnumWithSlice[] subDocsAndPositions;

  private BytesRef lastSeek;
  private boolean lastSeekExact;
  private final BytesRef lastSeekScratch = new BytesRef();

  private int numTop;
  private int numSubs;
  private BytesRef current;
  private Comparator<BytesRef> termComp;

  public static class TermsEnumIndex {
    public final static TermsEnumIndex[] EMPTY_ARRAY = new TermsEnumIndex[0];
    final int subIndex;
    final TermsEnum termsEnum;

    public TermsEnumIndex(TermsEnum termsEnum, int subIndex) {
      this.termsEnum = termsEnum;
      this.subIndex = subIndex;
    }
  }

  public int getMatchCount() {
    return numTop;
  }

  public TermsEnumWithSlice[] getMatchArray() {
    return top;
  }

  public MultiTermsEnum(ReaderUtil.Slice[] slices) {
    queue = new TermMergeQueue(slices.length);
    top = new TermsEnumWithSlice[slices.length];
    subs = new TermsEnumWithSlice[slices.length];
    subDocs = new MultiDocsEnum.EnumWithSlice[slices.length];
    subDocsAndPositions = new MultiDocsAndPositionsEnum.EnumWithSlice[slices.length];
    for(int i=0;i<slices.length;i++) {
      subs[i] = new TermsEnumWithSlice(i, slices[i]);
      subDocs[i] = new MultiDocsEnum.EnumWithSlice();
      subDocs[i].slice = slices[i];
      subDocsAndPositions[i] = new MultiDocsAndPositionsEnum.EnumWithSlice();
      subDocsAndPositions[i].slice = slices[i];
    }
    currentSubs = new TermsEnumWithSlice[slices.length];
  }

  @Override
  public BytesRef term() {
    return current;
  }

  @Override
  public Comparator<BytesRef> getComparator() {
    return termComp;
  }

  /** The terms array must be newly created TermsEnum, ie
   *  {@link TermsEnum#next} has not yet been called. */
  public TermsEnum reset(TermsEnumIndex[] termsEnumsIndex) throws IOException {
    assert termsEnumsIndex.length <= top.length;
    numSubs = 0;
    numTop = 0;
    termComp = null;
    queue.clear();
    for(int i=0;i<termsEnumsIndex.length;i++) {

      final TermsEnumIndex termsEnumIndex = termsEnumsIndex[i];
      assert termsEnumIndex != null;

      // init our term comp
      if (termComp == null) {
        queue.termComp = termComp = termsEnumIndex.termsEnum.getComparator();
      } else {
        // We cannot merge sub-readers that have
        // different TermComps
        final Comparator<BytesRef> subTermComp = termsEnumIndex.termsEnum.getComparator();
        if (subTermComp != null && !subTermComp.equals(termComp)) {
          throw new IllegalStateException("sub-readers have different BytesRef.Comparators: " + subTermComp + " vs " + termComp + "; cannot merge");
        }
      }

      final BytesRef term = termsEnumIndex.termsEnum.next();
      if (term != null) {
        final TermsEnumWithSlice entry = subs[termsEnumIndex.subIndex];
        entry.reset(termsEnumIndex.termsEnum, term);
        queue.add(entry);
        currentSubs[numSubs++] = entry;
      } else {
        // field has no terms
      }
    }

    if (queue.size() == 0) {
      return TermsEnum.EMPTY;
    } else {
      return this;
    }
  }

  @Override
  public boolean seekExact(BytesRef term, boolean useCache) throws IOException {
    queue.clear();
    numTop = 0;

    boolean seekOpt = false;
    if (lastSeek != null && termComp.compare(lastSeek, term) <= 0) {
      seekOpt = true;
    }

    lastSeek = null;
    lastSeekExact = true;

    for(int i=0;i<numSubs;i++) {
      final boolean status;
      // LUCENE-2130: if we had just seek'd already, prior
      // to this seek, and the new seek term is after the
      // previous one, don't try to re-seek this sub if its
      // current term is already beyond this new seek term.
      // Doing so is a waste because this sub will simply
      // seek to the same spot.
      if (seekOpt) {
        final BytesRef curTerm = currentSubs[i].current;
        if (curTerm != null) {
          final int cmp = termComp.compare(term, curTerm);
          if (cmp == 0) {
            status = true;
          } else if (cmp < 0) {
            status = false;
          } else {
            status = currentSubs[i].terms.seekExact(term, useCache);
          }
        } else {
          status = false;
        }
      } else {
        status = currentSubs[i].terms.seekExact(term, useCache);
      }

      if (status) {
        top[numTop++] = currentSubs[i];
        current = currentSubs[i].current = currentSubs[i].terms.term();
        assert term.equals(currentSubs[i].current);
      }
    }

    // if at least one sub had exact match to the requested
    // term then we found match
    return numTop > 0;
  }

  @Override
  public SeekStatus seekCeil(BytesRef term, boolean useCache) throws IOException {
    queue.clear();
    numTop = 0;
    lastSeekExact = false;

    boolean seekOpt = false;
    if (lastSeek != null && termComp.compare(lastSeek, term) <= 0) {
      seekOpt = true;
    }

    lastSeekScratch.copyBytes(term);
    lastSeek = lastSeekScratch;

    for(int i=0;i<numSubs;i++) {
      final SeekStatus status;
      // LUCENE-2130: if we had just seek'd already, prior
      // to this seek, and the new seek term is after the
      // previous one, don't try to re-seek this sub if its
      // current term is already beyond this new seek term.
      // Doing so is a waste because this sub will simply
      // seek to the same spot.
      if (seekOpt) {
        final BytesRef curTerm = currentSubs[i].current;
        if (curTerm != null) {
          final int cmp = termComp.compare(term, curTerm);
          if (cmp == 0) {
            status = SeekStatus.FOUND;
          } else if (cmp < 0) {
            status = SeekStatus.NOT_FOUND;
          } else {
            status = currentSubs[i].terms.seekCeil(term, useCache);
          }
        } else {
          status = SeekStatus.END;
        }
      } else {
        status = currentSubs[i].terms.seekCeil(term, useCache);
      }

      if (status == SeekStatus.FOUND) {
        top[numTop++] = currentSubs[i];
        current = currentSubs[i].current = currentSubs[i].terms.term();
      } else {
        if (status == SeekStatus.NOT_FOUND) {
          currentSubs[i].current = currentSubs[i].terms.term();
          assert currentSubs[i].current != null;
          queue.add(currentSubs[i]);
        } else {
          // enum exhausted
          currentSubs[i].current = null;
        }
      }
    }

    if (numTop > 0) {
      // at least one sub had exact match to the requested term
      return SeekStatus.FOUND;
    } else if (queue.size() > 0) {
      // no sub had exact match, but at least one sub found
      // a term after the requested term -- advance to that
      // next term:
      pullTop();
      return SeekStatus.NOT_FOUND;
    } else {
      return SeekStatus.END;
    }
  }

  @Override
  public void seekExact(long ord) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long ord() throws IOException {
    throw new UnsupportedOperationException();
  }

  private void pullTop() {
    // extract all subs from the queue that have the same
    // top term
    assert numTop == 0;
    while(true) {
      top[numTop++] = queue.pop();
      if (queue.size() == 0 || !(queue.top()).current.bytesEquals(top[0].current)) {
        break;
      }
    } 
    current = top[0].current;
  }

  private void pushTop() throws IOException {
    // call next() on each top, and put back into queue
    for(int i=0;i<numTop;i++) {
      top[i].current = top[i].terms.next();
      if (top[i].current != null) {
        queue.add(top[i]);
      } else {
        // no more fields in this reader
      }
    }
    numTop = 0;
  }

  @Override
  public BytesRef next() throws IOException {
    if (lastSeekExact) {
      // Must seekCeil at this point, so those subs that
      // didn't have the term can find the following term.
      // NOTE: we could save some CPU by only seekCeil the
      // subs that didn't match the last exact seek... but
      // most impls short-circuit if you seekCeil to term
      // they are already on.
      final SeekStatus status = seekCeil(current);
      assert status == SeekStatus.FOUND;
      lastSeekExact = false;
    }
    lastSeek = null;

    // restore queue
    pushTop();

    // gather equal top fields
    if (queue.size() > 0) {
      pullTop();
    } else {
      current = null;
    }

    return current;
  }

  @Override
  public int docFreq() throws IOException {
    int sum = 0;
    for(int i=0;i<numTop;i++) {
      sum += top[i].terms.docFreq();
    }
    return sum;
  }

  @Override
  public long totalTermFreq() throws IOException {
    long sum = 0;
    for(int i=0;i<numTop;i++) {
      final long v = top[i].terms.totalTermFreq();
      if (v == -1) {
        return v;
      }
      sum += v;
    }
    return sum;
  }

  @Override
  public DocsEnum docs(Bits liveDocs, DocsEnum reuse, boolean needsFreqs) throws IOException {
    MultiDocsEnum docsEnum;
    // Can only reuse if incoming enum is also a MultiDocsEnum
    if (reuse != null && reuse instanceof MultiDocsEnum) {
      docsEnum = (MultiDocsEnum) reuse;
      // ... and was previously created w/ this MultiTermsEnum:
      if (!docsEnum.canReuse(this)) {
        docsEnum = new MultiDocsEnum(this, subs.length);
      }
    } else {
      docsEnum = new MultiDocsEnum(this, subs.length);
    }
    
    final MultiBits multiLiveDocs;
    if (liveDocs instanceof MultiBits) {
      multiLiveDocs = (MultiBits) liveDocs;
    } else {
      multiLiveDocs = null;
    }

    int upto = 0;

    for(int i=0;i<numTop;i++) {

      final TermsEnumWithSlice entry = top[i];

      final Bits b;

      if (multiLiveDocs != null) {
        // optimize for common case: requested skip docs is a
        // congruent sub-slice of MultiBits: in this case, we
        // just pull the liveDocs from the sub reader, rather
        // than making the inefficient
        // Slice(Multi(sub-readers)):
        final MultiBits.SubResult sub = multiLiveDocs.getMatchingSub(entry.subSlice);
        if (sub.matches) {
          b = sub.result;
        } else {
          // custom case: requested skip docs is foreign:
          // must slice it on every access
          b = new BitsSlice(liveDocs, entry.subSlice);
        }
      } else if (liveDocs != null) {
        b = new BitsSlice(liveDocs, entry.subSlice);
      } else {
        // no deletions
        b = null;
      }

      assert entry.index < docsEnum.subDocsEnum.length: entry.index + " vs " + docsEnum.subDocsEnum.length + "; " + subs.length;
      final DocsEnum subDocsEnum = entry.terms.docs(b, docsEnum.subDocsEnum[entry.index], needsFreqs);
      if (subDocsEnum != null) {
        docsEnum.subDocsEnum[entry.index] = subDocsEnum;
        subDocs[upto].docsEnum = subDocsEnum;
        subDocs[upto].slice = entry.subSlice;
        upto++;
      } else {
        // One of our subs cannot provide freqs:
        assert needsFreqs;
        return null;
      }
    }

    if (upto == 0) {
      return null;
    } else {
      return docsEnum.reset(subDocs, upto);
    }
  }

  @Override
  public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, boolean needsOffsets) throws IOException {
    MultiDocsAndPositionsEnum docsAndPositionsEnum;
    // Can only reuse if incoming enum is also a MultiDocsAndPositionsEnum
    if (reuse != null && reuse instanceof MultiDocsAndPositionsEnum) {
      docsAndPositionsEnum = (MultiDocsAndPositionsEnum) reuse;
      // ... and was previously created w/ this MultiTermsEnum:
      if (!docsAndPositionsEnum.canReuse(this)) {
        docsAndPositionsEnum = new MultiDocsAndPositionsEnum(this, subs.length);
      }
    } else {
      docsAndPositionsEnum = new MultiDocsAndPositionsEnum(this, subs.length);
    }
    
    final MultiBits multiLiveDocs;
    if (liveDocs instanceof MultiBits) {
      multiLiveDocs = (MultiBits) liveDocs;
    } else {
      multiLiveDocs = null;
    }

    int upto = 0;

    for(int i=0;i<numTop;i++) {

      final TermsEnumWithSlice entry = top[i];

      final Bits b;

      if (multiLiveDocs != null) {
        // Optimize for common case: requested skip docs is a
        // congruent sub-slice of MultiBits: in this case, we
        // just pull the liveDocs from the sub reader, rather
        // than making the inefficient
        // Slice(Multi(sub-readers)):
        final MultiBits.SubResult sub = multiLiveDocs.getMatchingSub(top[i].subSlice);
        if (sub.matches) {
          b = sub.result;
        } else {
          // custom case: requested skip docs is foreign:
          // must slice it on every access (very
          // inefficient)
          b = new BitsSlice(liveDocs, top[i].subSlice);
        }
      } else if (liveDocs != null) {
        b = new BitsSlice(liveDocs, top[i].subSlice);
      } else {
        // no deletions
        b = null;
      }

      assert entry.index < docsAndPositionsEnum.subDocsAndPositionsEnum.length: entry.index + " vs " + docsAndPositionsEnum.subDocsAndPositionsEnum.length + "; " + subs.length;
      final DocsAndPositionsEnum subPostings = entry.terms.docsAndPositions(b, docsAndPositionsEnum.subDocsAndPositionsEnum[entry.index], needsOffsets);

      if (subPostings != null) {
        docsAndPositionsEnum.subDocsAndPositionsEnum[entry.index] = subPostings;
        subDocsAndPositions[upto].docsAndPositionsEnum = subPostings;
        subDocsAndPositions[upto].slice = entry.subSlice;
        upto++;
      } else {
        if (entry.terms.docs(b, null, false) != null) {
          // At least one of our subs does not store
          // offsets or positions -- we can't correctly
          // produce a MultiDocsAndPositions enum
          return null;
        }
      }
    }

    if (upto == 0) {
      return null;
    } else {
      return docsAndPositionsEnum.reset(subDocsAndPositions, upto);
    }
  }

  private final static class TermsEnumWithSlice {
    private final ReaderUtil.Slice subSlice;
    private TermsEnum terms;
    public BytesRef current;
    final int index;

    public TermsEnumWithSlice(int index, ReaderUtil.Slice subSlice) {
      this.subSlice = subSlice;
      this.index = index;
      assert subSlice.length >= 0: "length=" + subSlice.length;
    }

    public void reset(TermsEnum terms, BytesRef term) {
      this.terms = terms;
      current = term;
    }

    @Override
    public String toString() {
      return subSlice.toString()+":"+terms;
    }
  }

  private final static class TermMergeQueue extends PriorityQueue<TermsEnumWithSlice> {
    Comparator<BytesRef> termComp;
    TermMergeQueue(int size) {
      super(size);
    }

    @Override
    protected boolean lessThan(TermsEnumWithSlice termsA, TermsEnumWithSlice termsB) {
      final int cmp = termComp.compare(termsA.current, termsB.current);
      if (cmp != 0) {
        return cmp < 0;
      } else {
        return termsA.subSlice.start < termsB.subSlice.start;
      }
    }
  }

  @Override
  public String toString() {
    return "MultiTermsEnum(" + Arrays.toString(subs) + ")";
  }
}
