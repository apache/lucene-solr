package org.apache.lucene.index;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

/** This stores a monotonically increasing set of <Term, TermInfo> pairs in a
 * Directory.  Pairs are accessed either by Term or by ordinal position the
 * set.  */

final class TermInfosReader {
  private Directory directory;
  private String segment;
  private FieldInfos fieldInfos;

  private SegmentTermEnum enumerator;
  private long size;

  TermInfosReader(Directory dir, String seg, FieldInfos fis)
       throws IOException {
    directory = dir;
    segment = seg;
    fieldInfos = fis;

    enumerator = new SegmentTermEnum(directory.openFile(segment + ".tis"),
			       fieldInfos, false);
    size = enumerator.size;
    readIndex();
  }

  public int getSkipInterval() {
    return enumerator.skipInterval;
  }

  final void close() throws IOException {
    if (enumerator != null)
      enumerator.close();
  }

  /** Returns the number of term/value pairs in the set. */
  final long size() {
    return size;
  }

  Term[] indexTerms = null;
  TermInfo[] indexInfos;
  long[] indexPointers;

  private final void readIndex() throws IOException {
    SegmentTermEnum indexEnum =
      new SegmentTermEnum(directory.openFile(segment + ".tii"),
			  fieldInfos, true);
    try {
      int indexSize = (int)indexEnum.size;

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
    }
  }

  /** Returns the offset of the greatest index entry which is less than term.*/
  private final int getIndexOffset(Term term) throws IOException {
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
    enumerator.seek(indexPointers[indexOffset],
	      (indexOffset * enumerator.indexInterval) - 1,
	      indexTerms[indexOffset], indexInfos[indexOffset]);
  }

  /** Returns the TermInfo for a Term in the set, or null. */
  final synchronized TermInfo get(Term term) throws IOException {
    if (size == 0) return null;

    // optimize sequential access: first try scanning cached enumerator w/o seeking
    if (enumerator.term() != null                 // term is at or past current
	&& ((enumerator.prev != null && term.compareTo(enumerator.prev) > 0)
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
    while (term.compareTo(enumerator.term()) > 0 && enumerator.next()) {}
    if (enumerator.term() != null && term.compareTo(enumerator.term()) == 0)
      return enumerator.termInfo();
    else
      return null;
  }

  /** Returns the nth term in the set. */
  final synchronized Term get(int position) throws IOException {
    if (size == 0) return null;

    if (enumerator != null && enumerator.term() != null && position >= enumerator.position &&
	position < (enumerator.position + enumerator.indexInterval))
      return scanEnum(position);		  // can avoid seek

    seekEnum(position / enumerator.indexInterval); // must seek
    return scanEnum(position);
  }

  private final Term scanEnum(int position) throws IOException {
    while(enumerator.position < position)
      if (!enumerator.next())
	return null;

    return enumerator.term();
  }

  /** Returns the position of a Term in the set or -1. */
  final synchronized long getPosition(Term term) throws IOException {
    if (size == 0) return -1;

    int indexOffset = getIndexOffset(term);
    seekEnum(indexOffset);

    while(term.compareTo(enumerator.term()) > 0 && enumerator.next()) {}

    if (term.compareTo(enumerator.term()) == 0)
      return enumerator.position;
    else
      return -1;
  }

  /** Returns an enumeration of all the Terms and TermInfos in the set. */
  final synchronized SegmentTermEnum terms() throws IOException {
    if (enumerator.position != -1)			  // if not at start
      seekEnum(0);				  // reset to start
    return (SegmentTermEnum)enumerator.clone();
  }

  /** Returns an enumeration of terms starting at or after the named term. */
  final synchronized SegmentTermEnum terms(Term term) throws IOException {
    get(term);					  // seek enumerator to term
    return (SegmentTermEnum)enumerator.clone();
  }


}
