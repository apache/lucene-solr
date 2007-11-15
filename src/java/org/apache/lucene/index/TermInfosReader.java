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

import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.BufferedIndexInput;

/** This stores a monotonically increasing set of <Term, TermInfo> pairs in a
 * Directory.  Pairs are accessed either by Term or by ordinal position the
 * set.  */

final class TermInfosReader {
  private Directory directory;
  private String segment;
  private FieldInfos fieldInfos;

  private ThreadLocal enumerators = new ThreadLocal();
  private SegmentTermEnum origEnum;
  private long size;

  private Term[] indexTerms = null;
  private TermInfo[] indexInfos;
  private long[] indexPointers;
  
  private SegmentTermEnum indexEnum;
  
  private int indexDivisor = 1;
  private int totalIndexInterval;

  TermInfosReader(Directory dir, String seg, FieldInfos fis)
       throws CorruptIndexException, IOException {
    this(dir, seg, fis, BufferedIndexInput.BUFFER_SIZE);
  }

  TermInfosReader(Directory dir, String seg, FieldInfos fis, int readBufferSize)
       throws CorruptIndexException, IOException {
    boolean success = false;

    try {
      directory = dir;
      segment = seg;
      fieldInfos = fis;

      origEnum = new SegmentTermEnum(directory.openInput(segment + ".tis",
          readBufferSize), fieldInfos, false);
      size = origEnum.size;
      totalIndexInterval = origEnum.indexInterval;

      indexEnum = new SegmentTermEnum(directory.openInput(segment + ".tii",
          readBufferSize), fieldInfos, true);

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

  /**
   * <p>Sets the indexDivisor, which subsamples the number
   * of indexed terms loaded into memory.  This has a
   * similar effect as {@link
   * IndexWriter#setTermIndexInterval} except that setting
   * must be done at indexing time while this setting can be
   * set per reader.  When set to N, then one in every
   * N*termIndexInterval terms in the index is loaded into
   * memory.  By setting this to a value > 1 you can reduce
   * memory usage, at the expense of higher latency when
   * loading a TermInfo.  The default value is 1.</p>
   *
   * <b>NOTE:</b> you must call this before the term
   * index is loaded.  If the index is already loaded,
   * an IllegalStateException is thrown.
   *
   + @throws IllegalStateException if the term index has
   * already been loaded into memory.
   */
  public void setIndexDivisor(int indexDivisor) throws IllegalStateException {
    if (indexDivisor < 1)
      throw new IllegalArgumentException("indexDivisor must be > 0: got " + indexDivisor);

    if (indexTerms != null)
      throw new IllegalStateException("index terms are already loaded");

    this.indexDivisor = indexDivisor;
    totalIndexInterval = origEnum.indexInterval * indexDivisor;
  }

  /** Returns the indexDivisor.
   * @see #setIndexDivisor
   */
  public int getIndexDivisor() {
    return indexDivisor;
  }
  
  final void close() throws IOException {
    if (origEnum != null)
      origEnum.close();
    if (indexEnum != null)
      indexEnum.close();
    enumerators.set(null);
  }

  /** Returns the number of term/value pairs in the set. */
  final long size() {
    return size;
  }

  private SegmentTermEnum getEnum() {
    SegmentTermEnum termEnum = (SegmentTermEnum)enumerators.get();
    if (termEnum == null) {
      termEnum = terms();
      enumerators.set(termEnum);
    }
    return termEnum;
  }

  private synchronized void ensureIndexIsRead() throws IOException {
    if (indexTerms != null)                                    // index already read
      return;                                                  // do nothing
    try {
      int indexSize = 1+((int)indexEnum.size-1)/indexDivisor;  // otherwise read index

      indexTerms = new Term[indexSize];
      indexInfos = new TermInfo[indexSize];
      indexPointers = new long[indexSize];
        
      for (int i = 0; indexEnum.next(); i++) {
        indexTerms[i] = indexEnum.term();
        indexInfos[i] = indexEnum.termInfo();
        indexPointers[i] = indexEnum.indexPointer;
        
        for (int j = 1; j < indexDivisor; j++)
            if (!indexEnum.next())
                break;
      }
    } finally {
        indexEnum.close();
        indexEnum = null;
    }
  }

  /** Returns the offset of the greatest index entry which is less than or equal to term.*/
  private final int getIndexOffset(Term term) {
    int lo = 0;					  // binary search indexTerms[]
    int hi = indexTerms.length - 1;

    while (hi >= lo) {
      int mid = (lo + hi) >> 1;
      int delta = term.compareTo(indexTerms[mid]);
      if (delta < 0)
	hi = mid - 1;
      else if (delta > 0)
	lo = mid + 1;
      else
	return mid;
    }
    return hi;
  }

  private final void seekEnum(int indexOffset) throws IOException {
    getEnum().seek(indexPointers[indexOffset],
                   (indexOffset * totalIndexInterval) - 1,
                   indexTerms[indexOffset], indexInfos[indexOffset]);
  }

  /** Returns the TermInfo for a Term in the set, or null. */
  TermInfo get(Term term) throws IOException {
    if (size == 0) return null;

    ensureIndexIsRead();

    // optimize sequential access: first try scanning cached enum w/o seeking
    SegmentTermEnum enumerator = getEnum();
    if (enumerator.term() != null                 // term is at or past current
	&& ((enumerator.prev() != null && term.compareTo(enumerator.prev())> 0)
	    || term.compareTo(enumerator.term()) >= 0)) {
      int enumOffset = (int)(enumerator.position/totalIndexInterval)+1;
      if (indexTerms.length == enumOffset	  // but before end of block
	  || term.compareTo(indexTerms[enumOffset]) < 0)
	return scanEnum(term);			  // no need to seek
    }

    // random-access: must seek
    seekEnum(getIndexOffset(term));
    return scanEnum(term);
  }

  /** Scans within block for matching term. */
  private final TermInfo scanEnum(Term term) throws IOException {
    SegmentTermEnum enumerator = getEnum();
    enumerator.scanTo(term);
    if (enumerator.term() != null && term.compareTo(enumerator.term()) == 0)
      return enumerator.termInfo();
    else
      return null;
  }

  /** Returns the nth term in the set. */
  final Term get(int position) throws IOException {
    if (size == 0) return null;

    SegmentTermEnum enumerator = getEnum();
    if (enumerator != null && enumerator.term() != null &&
        position >= enumerator.position &&
	position < (enumerator.position + totalIndexInterval))
      return scanEnum(position);		  // can avoid seek

    seekEnum(position/totalIndexInterval); // must seek
    return scanEnum(position);
  }

  private final Term scanEnum(int position) throws IOException {
    SegmentTermEnum enumerator = getEnum();
    while(enumerator.position < position)
      if (!enumerator.next())
	return null;

    return enumerator.term();
  }

  /** Returns the position of a Term in the set or -1. */
  final long getPosition(Term term) throws IOException {
    if (size == 0) return -1;

    ensureIndexIsRead();
    int indexOffset = getIndexOffset(term);
    seekEnum(indexOffset);

    SegmentTermEnum enumerator = getEnum();
    while(term.compareTo(enumerator.term()) > 0 && enumerator.next()) {}

    if (term.compareTo(enumerator.term()) == 0)
      return enumerator.position;
    else
      return -1;
  }

  /** Returns an enumeration of all the Terms and TermInfos in the set. */
  public SegmentTermEnum terms() {
    return (SegmentTermEnum)origEnum.clone();
  }

  /** Returns an enumeration of terms starting at or after the named term. */
  public SegmentTermEnum terms(Term term) throws IOException {
    get(term);
    return (SegmentTermEnum)getEnum().clone();
  }
}
