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

  TermInfosReader(Directory dir, String seg, FieldInfos fis)
       throws CorruptIndexException, IOException {
    this(dir, seg, fis, BufferedIndexInput.BUFFER_SIZE);
  }

  TermInfosReader(Directory dir, String seg, FieldInfos fis, int readBufferSize)
       throws CorruptIndexException, IOException {
    directory = dir;
    segment = seg;
    fieldInfos = fis;

    origEnum = new SegmentTermEnum(directory.openInput(segment + ".tis", readBufferSize),
                                   fieldInfos, false);
    size = origEnum.size;

    indexEnum =
      new SegmentTermEnum(directory.openInput(segment + ".tii", readBufferSize),
			  fieldInfos, true);
  }

  public int getSkipInterval() {
    return origEnum.skipInterval;
  }
  
  public int getMaxSkipLevels() {
    return origEnum.maxSkipLevels;
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
    if (indexTerms != null)                       // index already read
      return;                                     // do nothing
    try {
      int indexSize = (int)indexEnum.size;        // otherwise read index

      indexTerms = new Term[indexSize];
      indexInfos = new TermInfo[indexSize];
      indexPointers = new long[indexSize];
        
      for (int i = 0; indexEnum.next(); i++) {
        indexTerms[i] = indexEnum.term();
        indexInfos[i] = indexEnum.termInfo();
        indexPointers[i] = indexEnum.indexPointer;
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
	      (indexOffset * getEnum().indexInterval) - 1,
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
      int enumOffset = (int)(enumerator.position/enumerator.indexInterval)+1;
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
	position < (enumerator.position + enumerator.indexInterval))
      return scanEnum(position);		  // can avoid seek

    seekEnum(position / enumerator.indexInterval); // must seek
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
