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
import org.apache.lucene.util.BitVector;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.store.IndexInput;

class SegmentTermDocs implements TermDocs {
  protected SegmentReader parent;
  protected IndexInput freqStream;
  protected int count;
  protected int df;
  protected BitVector deletedDocs;
  int doc = 0;
  int freq;

  private int skipInterval;
  private int maxSkipLevels;
  private DefaultSkipListReader skipListReader;
  
  private long freqBasePointer;
  private long proxBasePointer;

  private long skipPointer;
  private boolean haveSkipped;
  
  protected boolean currentFieldStoresPayloads;
  protected IndexOptions indexOptions;
  
  protected SegmentTermDocs(SegmentReader parent) {
    this.parent = parent;
    this.freqStream = (IndexInput) parent.core.freqStream.clone();
    synchronized (parent) {
      this.deletedDocs = parent.deletedDocs;
    }
    this.skipInterval = parent.core.getTermsReader().getSkipInterval();
    this.maxSkipLevels = parent.core.getTermsReader().getMaxSkipLevels();
  }

  public void seek(Term term) throws IOException {
    TermInfo ti = parent.core.getTermsReader().get(term);
    seek(ti, term);
  }

  public void seek(TermEnum termEnum) throws IOException {
    TermInfo ti;
    Term term;
    
    // use comparison of fieldinfos to verify that termEnum belongs to the same segment as this SegmentTermDocs
    if (termEnum instanceof SegmentTermEnum && ((SegmentTermEnum) termEnum).fieldInfos == parent.core.fieldInfos) {        // optimized case
      SegmentTermEnum segmentTermEnum = ((SegmentTermEnum) termEnum);
      term = segmentTermEnum.term();
      ti = segmentTermEnum.termInfo();
    } else  {                                         // punt case
      term = termEnum.term();
      ti = parent.core.getTermsReader().get(term);
    }
    
    seek(ti, term);
  }

  void seek(TermInfo ti, Term term) throws IOException {
    count = 0;
    FieldInfo fi = parent.core.fieldInfos.fieldInfo(term.field);
    indexOptions = (fi != null) ? fi.indexOptions : IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
    currentFieldStoresPayloads = (fi != null) ? fi.storePayloads : false;
    if (ti == null) {
      df = 0;
    } else {
      df = ti.docFreq;
      doc = 0;
      freqBasePointer = ti.freqPointer;
      proxBasePointer = ti.proxPointer;
      skipPointer = freqBasePointer + ti.skipOffset;
      freqStream.seek(freqBasePointer);
      haveSkipped = false;
    }
  }

  public void close() throws IOException {
    freqStream.close();
    if (skipListReader != null)
      skipListReader.close();
  }

  public final int doc() { return doc; }
  public final int freq() { return freq; }

  protected void skippingDoc() throws IOException {
  }

  public boolean next() throws IOException {
    while (true) {
      if (count == df)
        return false;
      final int docCode = freqStream.readVInt();
      
      if (indexOptions == IndexOptions.DOCS_ONLY) {
        doc += docCode;
        freq = 1;
      } else {
        doc += docCode >>> 1;       // shift off low bit
        if ((docCode & 1) != 0)       // if low bit is set
          freq = 1;         // freq is one
        else
          freq = freqStream.readVInt();     // else read freq
      }
      
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
    if (indexOptions == IndexOptions.DOCS_ONLY) {
      return readNoTf(docs, freqs, length);
    } else {
      int i = 0;
      while (i < length && count < df) {
        // manually inlined call to next() for speed
        final int docCode = freqStream.readVInt();
        doc += docCode >>> 1;       // shift off low bit
        if ((docCode & 1) != 0)       // if low bit is set
          freq = 1;         // freq is one
        else
          freq = freqStream.readVInt();     // else read freq
        count++;

        if (deletedDocs == null || !deletedDocs.get(doc)) {
          docs[i] = doc;
          freqs[i] = freq;
          ++i;
        }
      }
      return i;
    }
  }

  private final int readNoTf(final int[] docs, final int[] freqs, final int length) throws IOException {
    int i = 0;
    while (i < length && count < df) {
      // manually inlined call to next() for speed
      doc += freqStream.readVInt();       
      count++;

      if (deletedDocs == null || !deletedDocs.get(doc)) {
        docs[i] = doc;
        // Hardware freq to 1 when term freqs were not
        // stored in the index
        freqs[i] = 1;
        ++i;
      }
    }
    return i;
  }
 
  
  /** Overridden by SegmentTermPositions to skip in prox stream. */
  protected void skipProx(long proxPointer, int payloadLength) throws IOException {}

  /** Optimized implementation. */
  public boolean skipTo(int target) throws IOException {
    if ((target - skipInterval) >= doc && df >= skipInterval) {                      // optimized case
      if (skipListReader == null)
        skipListReader = new DefaultSkipListReader((IndexInput) freqStream.clone(), maxSkipLevels, skipInterval); // lazily clone

      if (!haveSkipped) {                          // lazily initialize skip stream
        skipListReader.init(skipPointer, freqBasePointer, proxBasePointer, df, currentFieldStoresPayloads);
        haveSkipped = true;
      }

      int newCount = skipListReader.skipTo(target); 
      if (newCount > count) {
        freqStream.seek(skipListReader.getFreqPointer());
        skipProx(skipListReader.getProxPointer(), skipListReader.getPayloadLength());

        doc = skipListReader.getDoc();
        count = newCount;
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
