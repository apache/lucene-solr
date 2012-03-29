package org.apache.lucene.codecs.lucene3x;

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

import java.io.IOException;
import java.util.Comparator;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CloseableThreadLocal;
import org.apache.lucene.util.DoubleBarrelLRUCache;

/** This stores a monotonically increasing set of <Term, TermInfo> pairs in a
 * Directory.  Pairs are accessed either by Term or by ordinal position the
 * set
 * @deprecated (4.0) This class has been replaced by
 * FormatPostingsTermsDictReader, except for reading old segments. 
 * @lucene.experimental
 */
@Deprecated
final class TermInfosReader {
  private final Directory directory;
  private final String segment;
  private final FieldInfos fieldInfos;

  private final CloseableThreadLocal<ThreadResources> threadResources = new CloseableThreadLocal<ThreadResources>();
  private final SegmentTermEnum origEnum;
  private final long size;

  private final TermInfosReaderIndex index;
  private final int indexLength;
  
  private final int totalIndexInterval;

  private final static int DEFAULT_CACHE_SIZE = 1024;
  
  // Just adds term's ord to TermInfo
  private final static class TermInfoAndOrd extends TermInfo {
    final long termOrd;
    public TermInfoAndOrd(TermInfo ti, long termOrd) {
      super(ti);
      assert termOrd >= 0;
      this.termOrd = termOrd;
    }
  }

  private static class CloneableTerm extends DoubleBarrelLRUCache.CloneableKey {
    Term term;
    public CloneableTerm(Term t) {
      this.term = t;
    }

    @Override
    public boolean equals(Object other) {
      CloneableTerm t = (CloneableTerm) other;
      return this.term.equals(t.term);
    }

    @Override
    public int hashCode() {
      return term.hashCode();
    }

    @Override
    public CloneableTerm clone() {
      return new CloneableTerm(term);
    }
  }

  private final DoubleBarrelLRUCache<CloneableTerm,TermInfoAndOrd> termsCache = new DoubleBarrelLRUCache<CloneableTerm,TermInfoAndOrd>(DEFAULT_CACHE_SIZE);

  /**
   * Per-thread resources managed by ThreadLocal
   */
  private static final class ThreadResources {
    SegmentTermEnum termEnum;
  }
  
  TermInfosReader(Directory dir, String seg, FieldInfos fis, IOContext context, int indexDivisor)
       throws CorruptIndexException, IOException {
    boolean success = false;

    if (indexDivisor < 1 && indexDivisor != -1) {
      throw new IllegalArgumentException("indexDivisor must be -1 (don't load terms index) or greater than 0: got " + indexDivisor);
    }

    try {
      directory = dir;
      segment = seg;
      fieldInfos = fis;

      origEnum = new SegmentTermEnum(directory.openInput(IndexFileNames.segmentFileName(segment, "", Lucene3xPostingsFormat.TERMS_EXTENSION),
                                                         context), fieldInfos, false);
      size = origEnum.size;


      if (indexDivisor != -1) {
        // Load terms index
        totalIndexInterval = origEnum.indexInterval * indexDivisor;

        final String indexFileName = IndexFileNames.segmentFileName(segment, "", Lucene3xPostingsFormat.TERMS_INDEX_EXTENSION);
        final SegmentTermEnum indexEnum = new SegmentTermEnum(directory.openInput(indexFileName,
                                                                                   context), fieldInfos, true);

        try {
          index = new TermInfosReaderIndex(indexEnum, indexDivisor, dir.fileLength(indexFileName), totalIndexInterval);
          indexLength = index.length();
        } finally {
          indexEnum.close();
        }
      } else {
        // Do not load terms index:
        totalIndexInterval = -1;
        index = null;
        indexLength = -1;
      }
      success = true;
    } finally {
      // With lock-less commits, it's entirely possible (and
      // fine) to hit a FileNotFound exception above. In
      // this case, we want to explicitly close any subset
      // of things that were opened so that we don't have to
      // wait for a GC to do so.
      if (!success) {
        close();
      }
    }
  }

  public int getSkipInterval() {
    return origEnum.skipInterval;
  }
  
  public int getMaxSkipLevels() {
    return origEnum.maxSkipLevels;
  }

  void close() throws IOException {
    if (origEnum != null)
      origEnum.close();
    threadResources.close();
  }

  /** Returns the number of term/value pairs in the set. */
  long size() {
    return size;
  }

  private ThreadResources getThreadResources() {
    ThreadResources resources = threadResources.get();
    if (resources == null) {
      resources = new ThreadResources();
      resources.termEnum = terms();
      threadResources.set(resources);
    }
    return resources;
  }
  
  private static final Comparator<BytesRef> legacyComparator = 
    BytesRef.getUTF8SortedAsUTF16Comparator();

  private final int compareAsUTF16(Term term1, Term term2) {
    if (term1.field().equals(term2.field())) {
      return legacyComparator.compare(term1.bytes(), term2.bytes());
    } else {
      return term1.field().compareTo(term2.field());
    }
  }

  /** Returns the TermInfo for a Term in the set, or null. */
  TermInfo get(Term term) throws IOException {
    return get(term, false);
  }
  
  /** Returns the TermInfo for a Term in the set, or null. */
  private TermInfo get(Term term, boolean mustSeekEnum) throws IOException {
    if (size == 0) return null;

    ensureIndexIsRead();
    TermInfoAndOrd tiOrd = termsCache.get(new CloneableTerm(term));
    ThreadResources resources = getThreadResources();

    if (!mustSeekEnum && tiOrd != null) {
      return tiOrd;
    }

    return seekEnum(resources.termEnum, term, tiOrd, true);
  }

  public void cacheCurrentTerm(SegmentTermEnum enumerator) {
    termsCache.put(new CloneableTerm(enumerator.term()),
                   new TermInfoAndOrd(enumerator.termInfo,
                                      enumerator.position));
  }

  TermInfo seekEnum(SegmentTermEnum enumerator, Term term, boolean useCache) throws IOException {
    if (useCache) {
      return seekEnum(enumerator, term,
                      termsCache.get(new CloneableTerm(term.deepCopyOf())),
                      useCache);
    } else {
      return seekEnum(enumerator, term, null, useCache);
    }
  }

  TermInfo seekEnum(SegmentTermEnum enumerator, Term term, TermInfoAndOrd tiOrd, boolean useCache) throws IOException {
    if (size == 0) {
      return null;
    }

    // optimize sequential access: first try scanning cached enum w/o seeking
    if (enumerator.term() != null                 // term is at or past current
  && ((enumerator.prev() != null && compareAsUTF16(term, enumerator.prev())> 0)
      || compareAsUTF16(term, enumerator.term()) >= 0)) {
      int enumOffset = (int)(enumerator.position/totalIndexInterval)+1;
      if (indexLength == enumOffset    // but before end of block
    || index.compareTo(term, enumOffset) < 0) {
       // no need to seek

        final TermInfo ti;
        int numScans = enumerator.scanTo(term);
        if (enumerator.term() != null && compareAsUTF16(term, enumerator.term()) == 0) {
          ti = enumerator.termInfo;
          if (numScans > 1) {
            // we only  want to put this TermInfo into the cache if
            // scanEnum skipped more than one dictionary entry.
            // This prevents RangeQueries or WildcardQueries to 
            // wipe out the cache when they iterate over a large numbers
            // of terms in order
            if (tiOrd == null) {
              if (useCache) {
                termsCache.put(new CloneableTerm(term.deepCopyOf()),
                               new TermInfoAndOrd(ti, enumerator.position));
              }
            } else {
              assert sameTermInfo(ti, tiOrd, enumerator);
              assert (int) enumerator.position == tiOrd.termOrd;
            }
          }
        } else {
          ti = null;
        }

        return ti;
      }  
    }

    // random-access: must seek
    final int indexPos;
    if (tiOrd != null) {
      indexPos = (int) (tiOrd.termOrd / totalIndexInterval);
    } else {
      // Must do binary search:
      indexPos = index.getIndexOffset(term);
    }

    index.seekEnum(enumerator, indexPos);
    enumerator.scanTo(term);
    final TermInfo ti;

    if (enumerator.term() != null && compareAsUTF16(term, enumerator.term()) == 0) {
      ti = enumerator.termInfo;
      if (tiOrd == null) {
        if (useCache) {
          termsCache.put(new CloneableTerm(term.deepCopyOf()),
                         new TermInfoAndOrd(ti, enumerator.position));
        }
      } else {
        assert sameTermInfo(ti, tiOrd, enumerator);
        assert enumerator.position == tiOrd.termOrd;
      }
    } else {
      ti = null;
    }
    return ti;
  }

  // called only from asserts
  private boolean sameTermInfo(TermInfo ti1, TermInfo ti2, SegmentTermEnum enumerator) {
    if (ti1.docFreq != ti2.docFreq) {
      return false;
    }
    if (ti1.freqPointer != ti2.freqPointer) {
      return false;
    }
    if (ti1.proxPointer != ti2.proxPointer) {
      return false;
    }
    // skipOffset is only valid when docFreq >= skipInterval:
    if (ti1.docFreq >= enumerator.skipInterval &&
        ti1.skipOffset != ti2.skipOffset) {
      return false;
    }
    return true;
  }

  private void ensureIndexIsRead() {
    if (index == null) {
      throw new IllegalStateException("terms index was not loaded when this reader was created");
    }
  }

  /** Returns the position of a Term in the set or -1. */
  long getPosition(Term term) throws IOException {
    if (size == 0) return -1;

    ensureIndexIsRead();
    int indexOffset = index.getIndexOffset(term);
    
    SegmentTermEnum enumerator = getThreadResources().termEnum;
    index.seekEnum(enumerator, indexOffset);

    while(compareAsUTF16(term, enumerator.term()) > 0 && enumerator.next()) {}

    if (compareAsUTF16(term, enumerator.term()) == 0)
      return enumerator.position;
    else
      return -1;
  }

  /** Returns an enumeration of all the Terms and TermInfos in the set. */
  public SegmentTermEnum terms() {
    return origEnum.clone();
  }

  /** Returns an enumeration of terms starting at or after the named term. */
  public SegmentTermEnum terms(Term term) throws IOException {
    get(term, true);
    return getThreadResources().termEnum.clone();
  }
}
