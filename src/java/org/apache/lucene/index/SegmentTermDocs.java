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
import org.apache.lucene.util.BitVector;
import org.apache.lucene.store.InputStream;

class SegmentTermDocs implements TermDocs {
  protected SegmentReader parent;
  private InputStream freqStream;
  private int count;
  private int df;
  private BitVector deletedDocs;
  int doc = 0;
  int freq;

  private int skipInterval;
  private int numSkips;
  private int skipCount;
  private InputStream skipStream;
  private int skipDoc;
  private long freqPointer;
  private long proxPointer;
  private long skipPointer;
  private boolean haveSkipped;

  SegmentTermDocs(SegmentReader parent)
          throws IOException {
    this.parent = parent;
    this.freqStream = (InputStream) parent.freqStream.clone();
    this.deletedDocs = parent.deletedDocs;
    this.skipInterval = parent.tis.getSkipInterval();
  }

  public void seek(Term term) throws IOException {
    TermInfo ti = parent.tis.get(term);
    seek(ti);
  }

  public void seek(TermEnum enum) throws IOException {
    TermInfo ti;
    
    // use comparison of fieldinfos to verify that enum belongs to the same segment as this SegmentTermDocs
    if (enum instanceof SegmentTermEnum && ((SegmentTermEnum) enum).fieldInfos == parent.fieldInfos)          // optimized case
      ti = ((SegmentTermEnum) enum).termInfo();
    else                                          // punt case
      ti = parent.tis.get(enum.term());
      
    seek(ti);
  }

  void seek(TermInfo ti) throws IOException {
    count = 0;
    if (ti == null) {
      df = 0;
    } else {
      df = ti.docFreq;
      doc = 0;
      skipDoc = 0;
      skipCount = 0;
      numSkips = df / skipInterval;
      freqPointer = ti.freqPointer;
      proxPointer = ti.proxPointer;
      skipPointer = freqPointer + ti.skipOffset;
      freqStream.seek(freqPointer);
      haveSkipped = false;
    }
  }

  public void close() throws IOException {
    freqStream.close();
  }

  public final int doc() { return doc; }
  public final int freq() { return freq; }

  protected void skippingDoc() throws IOException {
  }

  public boolean next() throws IOException {
    while (true) {
      if (count == df)
        return false;

      int docCode = freqStream.readVInt();
      doc += docCode >>> 1;			  // shift off low bit
      if ((docCode & 1) != 0)			  // if low bit is set
        freq = 1;				  // freq is one
      else
        freq = freqStream.readVInt();		  // else read freq

      count++;

      if (deletedDocs == null || !deletedDocs.get(doc))
        break;
      skippingDoc();
    }
    return true;
  }

  /** Optimized implementation. */
  public int read(final int[] docs, final int[] freqs)
          throws IOException {
    final int length = docs.length;
    int i = 0;
    while (i < length && count < df) {

      // manually inlined call to next() for speed
      final int docCode = freqStream.readVInt();
      doc += docCode >>> 1;			  // shift off low bit
      if ((docCode & 1) != 0)			  // if low bit is set
        freq = 1;				  // freq is one
      else
        freq = freqStream.readVInt();		  // else read freq
      count++;

      if (deletedDocs == null || !deletedDocs.get(doc)) {
        docs[i] = doc;
        freqs[i] = freq;
        ++i;
      }
    }
    return i;
  }

  /** Overridden by SegmentTermPositions to skip in prox stream. */
  protected void skipProx(long proxPointer) throws IOException {}

  /** Optimized implementation. */
  public boolean skipTo(int target) throws IOException {
    if (df > skipInterval) {                      // optimized case

      if (skipStream == null)
        skipStream = (InputStream) freqStream.clone(); // lazily clone

      if (!haveSkipped) {                          // lazily seek skip stream
        skipStream.seek(skipPointer);
        haveSkipped = true;
      }

      // scan skip data
      int lastSkipDoc = skipDoc;
      long lastFreqPointer = freqStream.getFilePointer();
      long lastProxPointer = -1;
      int numSkipped = -1 - (count % skipInterval);

      while (target > skipDoc && skipCount < numSkips) {
        lastSkipDoc = skipDoc;
        lastFreqPointer = freqPointer;
        lastProxPointer = proxPointer;

        if (skipDoc != 0 && skipDoc >= doc)
          numSkipped += skipInterval;

        skipDoc += skipStream.readVInt();
        freqPointer += skipStream.readVInt();
        proxPointer += skipStream.readVInt();

        skipCount++;
      }
      
      // if we found something to skip, then skip it
      if (lastFreqPointer > freqStream.getFilePointer()) {
        freqStream.seek(lastFreqPointer);
        skipProx(lastProxPointer);

        doc = lastSkipDoc;
        count += numSkipped;
      }

    }

    // done skipping, now just scan
    do {
      if (!next())
        return false;
    } while (target > doc);
    return true;
  }

}
